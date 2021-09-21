# frozen_string_literal: true

module Delayed
  module WorkQueue
    class ParentProcess
      class Server
        attr_reader :clients, :listen_socket

        include Delayed::Logging
        SIGNALS = %i[INT TERM QUIT].freeze

        def initialize(listen_socket, parent_pid: nil, config: Settings.parent_process)
          @listen_socket = listen_socket
          @parent_pid = parent_pid
          @clients = {}
          @waiting_clients = {}
          @prefetched_jobs = {}

          @config = config
          @client_timeout = config["server_socket_timeout"] || 10.0 # left for backwards compat

          @exit = false
          @self_pipe = IO.pipe
        end

        def connected_clients
          @clients.size
        end

        def all_workers_idle?
          @clients.none? { |_, c| c.working }
        end

        # run the server queue worker
        # this method does not return, only exits or raises an exception
        def run
          logger.debug "Starting work queue process"

          SIGNALS.each do |sig|
            # We're not doing any aggressive exiting here since we really want
            # prefetched jobs to be unlocked and we're going to wake up the process
            # from the IO.select we're using to wait on clients.
            trap(sig) do
              @exit = true
              @self_pipe[1].write_nonblock(".", exception: false)
            end
          end

          last_orphaned_prefetched_jobs_purge = Job.db_time_now - rand(15 * 60)
          until exit?
            run_once
            if last_orphaned_prefetched_jobs_purge + (15 * 60) < Job.db_time_now
              Job.unlock_orphaned_prefetched_jobs
              last_orphaned_prefetched_jobs_purge = Job.db_time_now
            end
          end
        rescue => e
          logger.error "WorkQueue Server died: #{e.inspect}"
          raise
        ensure
          unlock_all_prefetched_jobs
        end

        def run_once
          handles = @clients.keys + [@listen_socket, @self_pipe[0]]
          # if we're currently idle, then force a "latency" to job fetching - don't
          # fetch recently queued jobs, allowing busier workers to fetch them first.
          # if they're not keeping up, the jobs will slip back in time, and suddenly we'll become
          # active and quickly pick up all the jobs we can. The latency is calculated to ensure that
          # an active worker is guaranteed to have attempted to fetch new jobs in the meantime
          forced_latency = Settings.sleep_delay + (Settings.sleep_delay_stagger * 2) if all_workers_idle?
          timeout = Settings.sleep_delay + (rand * Settings.sleep_delay_stagger)
          readable, = IO.select(handles, nil, nil, timeout)
          readable&.each { |s| handle_read(s) }
          Delayed::Worker.lifecycle.run_callbacks(:check_for_work, self) do
            check_for_work(forced_latency: forced_latency)
          end
          unlock_timed_out_prefetched_jobs
        end

        def handle_read(socket)
          if socket == @listen_socket
            handle_accept
          elsif socket == @self_pipe[0]
            # We really don't care about the contents of the pipe, we just need to
            # wake up.
            @self_pipe[0].read_nonblock(11, exception: false)
          else
            handle_request(socket)
          end
        end

        # Any error on the listen socket other than WaitReadable will bubble up
        # and terminate the work queue process, to be restarted by the parent daemon.
        def handle_accept
          socket, _addr = @listen_socket.accept_nonblock
          @clients[socket] = ClientState.new(false, socket) if socket
        rescue IO::WaitReadable
          logger.error("Server attempted to read listen_socket but failed with IO::WaitReadable")
          # ignore and just try accepting again next time through the loop
        end

        def handle_request(socket)
          # There is an assumption here that the client will never send a partial
          # request and then leave the socket open. Doing so would leave us hanging
          # in Marshal.load forever. This is only a reasonable assumption because we
          # control the client.
          client = @clients[socket]
          if socket.eof?
            logger.debug("Client #{client.name} closed connection")
            return drop_socket(socket)
          end
          worker_name, worker_config = Marshal.load(socket)
          client.name = worker_name
          client.working = false
          (@waiting_clients[worker_config] ||= []) << client
        rescue SystemCallError, IOError => e
          logger.error("Receiving message from client (#{socket}) failed: #{e.inspect}")
          drop_socket(socket)
        end

        def check_for_work(forced_latency: nil)
          @waiting_clients.each do |(worker_config, workers)|
            prefetched_jobs = @prefetched_jobs[worker_config] ||= []
            logger.debug("I have #{prefetched_jobs.length} jobs for #{workers.length} waiting workers")
            while !prefetched_jobs.empty? && !workers.empty?
              job = prefetched_jobs.shift
              client = workers.shift
              # couldn't re-lock it for some reason
              logger.debug("Transferring prefetched job to #{client.name}")
              unless job.transfer_lock!(from: prefetch_owner, to: client.name)
                workers.unshift(client)
                next
              end
              client.working = true
              begin
                logger.debug("Sending prefetched job #{job.id} to #{client.name}")
                client_timeout { Marshal.dump(job, client.socket) }
              rescue SystemCallError, IOError, Timeout::Error => e
                logger.error("Failed to send pre-fetched job to #{client.name}: #{e.inspect}")
                drop_socket(client.socket)
                Delayed::Job.unlock([job])
              end
            end

            next if workers.empty?

            logger.debug("Fetching new work for #{workers.length} workers")
            jobs_to_send = []

            Delayed::Worker.lifecycle.run_callbacks(:work_queue_pop, self, worker_config) do
              recipients = workers.map(&:name)

              response = Delayed::Job.get_and_lock_next_available(
                recipients,
                worker_config[:queue],
                worker_config[:min_priority],
                worker_config[:max_priority],
                prefetch: (Settings.fetch_batch_size * (worker_config[:workers] || 1)) - recipients.length,
                prefetch_owner: prefetch_owner,
                forced_latency: forced_latency
              )
              logger.debug(
                "Fetched and locked #{response.values.flatten.size} new jobs for workers (#{response.keys.join(', ')})."
              )
              response.each do |(worker_name, locked_jobs)|
                if worker_name == prefetch_owner
                  # it's actually an array of all the extra jobs
                  logger.debug(
                    "Adding prefetched jobs #{locked_jobs.length} to prefetched array (size: #{prefetched_jobs.count})"
                  )
                  prefetched_jobs.concat(locked_jobs)
                  next
                end
                client = workers.find { |worker| worker.name == worker_name }
                client.working = true
                jobs_to_send << [client, locked_jobs]
              end
            end

            jobs_to_send.each do |(recipient, job_to_send)|
              @waiting_clients[worker_config].delete(client)
              begin
                logger.debug("Sending job #{job_to_send.id} to #{recipient.name}")
                client_timeout { Marshal.dump(job_to_send, recipient.socket) }
              rescue SystemCallError, IOError, Timeout::Error => e
                logger.error("Failed to send job to #{recipient.name}: #{e.inspect}")
                drop_socket(recipient.socket)
                Delayed::Job.unlock([job_to_send])
              end
            end
          end
        end

        def unlock_timed_out_prefetched_jobs
          @prefetched_jobs.each do |(worker_config, jobs)|
            next if jobs.empty?
            next unless jobs.first.locked_at < Time.now.utc - Settings.parent_process[:prefetched_jobs_timeout]

            Delayed::Job.transaction do
              Delayed::Job.connection.execute("SELECT pg_advisory_xact_lock('#{Delayed::Job.prefetch_jobs_lock_name}')")
              Delayed::Job.unlock(jobs)
            end
            @prefetched_jobs[worker_config] = []
          end
        end

        def unlock_all_prefetched_jobs
          @prefetched_jobs.each do |(_worker_config, jobs)|
            next if jobs.empty?

            Delayed::Job.transaction do
              Delayed::Job.connection.execute("SELECT pg_advisory_xact_lock('#{Delayed::Job.prefetch_jobs_lock_name}')")
              Delayed::Job.unlock(jobs)
            end
          end
          @prefetched_jobs = {}
        end

        def drop_socket(socket)
          # this socket went away
          begin
            socket.close
          rescue IOError
            nil
          end
          client = @clients[socket]
          @clients.delete(socket)
          @waiting_clients.each do |(_config, workers)|
            workers.delete(client)
          end
        end

        def exit?
          !!@exit || parent_exited?
        end

        def prefetch_owner
          "prefetch:#{Socket.gethostname rescue 'X'}"
        end

        def parent_exited?
          @parent_pid && @parent_pid != Process.ppid
        end

        def client_timeout(&block)
          Timeout.timeout(@client_timeout, &block)
        end

        ClientState = Struct.new(:working, :socket, :name)
      end
    end
  end
end
