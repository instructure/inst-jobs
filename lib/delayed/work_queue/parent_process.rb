require 'pathname'
require 'socket'
require 'timeout'

module Delayed
module WorkQueue
# ParentProcess is a WorkQueue implementation that spawns a separate worker
# process for querying the queue. Each Worker child process sends requests to
# the ParentProcess via IPC, and receives responses. This centralized queue
# querying cuts down on db queries and lock contention, and allows the
# possibility for other centralized logic such as notifications when all workers
# are idle.
#
# The IPC implementation uses Unix stream sockets and Ruby's built-in Marshal
# functionality. The ParentProcess creates a Unix socket on the filesystem in
# the tmp directory, so that if a worker process dies and is restarted it can
# reconnect to the socket.
#
# While Unix and IP sockets are API compatible, we take a lot of shortcuts
# because we know it's just a local Unix socket. If we ever wanted to swap this
# out for a TCP/IP socket and have the WorkQueue running on another host, we'd
# want to be a lot more robust about partial reads/writes and timeouts.
class ParentProcess
  class ProtocolError < RuntimeError
  end

  attr_reader :server_address

  DEFAULT_SOCKET_NAME = 'inst-jobs.sock'.freeze
  private_constant :DEFAULT_SOCKET_NAME

  def initialize(config = Settings.parent_process)
    @config = config
    @server_address = generate_socket_path(config['server_address'])
  end

  def server(parent_pid: nil)
    # The unix_server_socket method takes care of cleaning up any existing
    # socket for us if the work queue process dies and is restarted.
    listen_socket = Socket.unix_server_socket(@server_address)
    Server.new(listen_socket, parent_pid: parent_pid, config: @config)
  end

  def client
    Client.new(Addrinfo.unix(@server_address))
  end

  private

  def generate_socket_path(supplied_path)
    pathname = Pathname.new(supplied_path)

    if pathname.absolute? && pathname.directory?
      pathname.join(DEFAULT_SOCKET_NAME).to_s
    elsif pathname.absolute?
      supplied_path
    else
      generate_socket_path(Settings.expand_rails_path(supplied_path))
    end
  end

  module SayUtil
    def say(msg, level = :debug)
      if defined?(Rails.logger) && Rails.logger
        message = -> { "[#{Process.pid}]Q #{msg}" }
        Rails.logger.send(level, self.class.name, &message)
      else
        puts(msg)
      end
    end
  end

  class Client
    attr_reader :addrinfo

    include SayUtil

    def initialize(addrinfo)
      @addrinfo = addrinfo
    end

    def get_and_lock_next_available(worker_name, worker_config)
      @socket ||= @addrinfo.connect
      say("Requesting work using #{@socket.inspect}")
      Marshal.dump([worker_name, worker_config], @socket)
      response = Marshal.load(@socket)
      unless response.nil? || (response.is_a?(Delayed::Job) && response.locked_by == worker_name)
        say("Received invalid response from server: #{response.inspect}")
        raise(ProtocolError, "response is not a locked job: #{response.inspect}")
      end
      say("Received work from server: #{response.inspect}")
      response
    rescue SystemCallError, IOError => ex
      say("Work queue connection lost, reestablishing on next poll. (#{ex})", :error)
      # The work queue process died. Return nil to signal the worker
      # process should sleep as if no job was found, and then retry.
      @socket = nil
      nil
    end
  end

  class Server
    attr_reader :listen_socket

    include SayUtil

    def initialize(listen_socket, parent_pid: nil, config: Settings.parent_process)
      @config = config
      @listen_socket = listen_socket
      @parent_pid = parent_pid
      @clients = {}
      @waiting_clients = {}
      @pending_work = {}
    end

    def connected_clients
      @clients.size
    end

    def all_workers_idle?
      !@clients.any? { |_, c| c.working }
    end

    # run the server queue worker
    # this method does not return, only exits or raises an exception
    def run
      say "Starting work queue process"

      last_orphaned_pending_jobs_purge = Job.db_time_now - rand(15 * 60)
      while !exit?
        run_once
        if last_orphaned_pending_jobs_purge + 15 * 60 < Job.db_time_now
          Job.unlock_orphaned_pending_jobs
          last_orphaned_pending_jobs_purge = Job.db_time_now
        end
      end
      purge_all_pending_work

    rescue => e
      say "WorkQueue Server died: #{e.inspect}", :error
      raise
    end

    def run_once
      handles = @clients.keys + [@listen_socket]
      timeout = Settings.sleep_delay + (rand * Settings.sleep_delay_stagger)
      readable, _, _ = IO.select(handles, nil, nil, timeout)
      if readable
        readable.each { |s| handle_read(s) }
      end
      check_for_work
      purge_extra_pending_work
    end

    def handle_read(socket)
      if socket == @listen_socket
        handle_accept
      else
        handle_request(socket)
      end
    end

    # Any error on the listen socket other than WaitReadable will bubble up
    # and terminate the work queue process, to be restarted by the parent daemon.
    def handle_accept
      socket, _addr = @listen_socket.accept_nonblock
      if socket
        @clients[socket] = ClientState.new(false, socket)
      end
    rescue IO::WaitReadable
      say("Server attempted to read listen_socket but failed with IO::WaitReadable", :error)
      # ignore and just try accepting again next time through the loop
    end

    def handle_request(socket)
      # There is an assumption here that the client will never send a partial
      # request and then leave the socket open. Doing so would leave us hanging
      # here forever. This is only a reasonable assumption because we control
      # the client.
      worker_name, worker_config = client_timeout { Marshal.load(socket) }
      client = @clients[socket]
      client.name = worker_name
      client.working = false
      (@waiting_clients[worker_config] ||= []) << client

    rescue SystemCallError, IOError, Timeout::Error => ex
      say("Receiving message from client (#{socket}) failed: #{ex.inspect}", :error)
      drop_socket(socket)
    end

    def pending_jobs_owner
      "work_queue:#{Socket.gethostname rescue 'X'}"
    end

    def check_for_work
      @waiting_clients.each do |(worker_config, workers)|
        pending_work = @pending_work[worker_config] ||= []
        say("I have #{pending_work.length} jobs for #{workers.length} waiting workers")
        while !pending_work.empty? && !workers.empty?
          job = pending_work.shift
          client = workers.shift
          # couldn't re-lock it for some reason
          unless job.transfer_lock!(from: pending_jobs_owner, to: client.name)
            workers.unshift(client)
            next
          end
          begin
            client_timeout { Marshal.dump(job, client.socket) }
          rescue SystemCallError, IOError, Timeout::Error
            drop_socket(client.socket)
          end
        end

        next if workers.empty?

        Delayed::Worker.lifecycle.run_callbacks(:work_queue_pop, self, worker_config) do
          recipients = workers.map(&:name)

          response = Delayed::Job.get_and_lock_next_available(
              recipients,
              worker_config[:queue],
              worker_config[:min_priority],
              worker_config[:max_priority],
              extra_jobs: Settings.fetch_batch_size * (worker_config[:workers] || 1) - recipients.length,
              extra_jobs_owner: pending_jobs_owner)
          response.each do |(worker_name, job)|
            if worker_name == pending_jobs_owner
              # it's actually an array of all the extra jobs
              pending_work.concat(job)
              next
            end
            client = workers.find { |worker| worker.name == worker_name }
            client.working = true
            @waiting_clients[worker_config].delete(client)
            begin
              client_timeout { Marshal.dump(job, client.socket) }
            rescue SystemCallError, IOError, Timeout::Error
              drop_socket(client.socket)
            end
          end
        end
      end
    end

    def purge_extra_pending_work
      @pending_work.each do |(worker_config, jobs)|
        next if jobs.empty?
        if jobs.first.locked_at < Time.now.utc - Settings.parent_process[:pending_jobs_idle_timeout]
          Delayed::Job.unlock(jobs)
          @pending_work[worker_config] = []
        end
      end
    end

    def purge_all_pending_work
      @pending_work.each do |(_worker_config, jobs)|
        next if jobs.empty?
        Delayed::Job.unlock(jobs)
      end
      @pending_work = {}
    end

    def drop_socket(socket)
      # this socket went away
      begin
        socket.close
      rescue IOError
      end
      @clients.delete(socket)
    end

    def exit?
      parent_exited?
    end

    def parent_exited?
      @parent_pid && @parent_pid != Process.ppid
    end

    def client_timeout
      Timeout.timeout(@config['server_socket_timeout']) { yield }
    end

    ClientState = Struct.new(:working, :socket, :name)
  end
end
end
end

