require 'socket'
require 'tempfile'
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

  def initialize
    @path = self.class.generate_socket_path
  end

  def self.generate_socket_path
    # We utilize Tempfile as a convenient way to get a socket filename in the
    # writeable temp directory. However, since we destroy the normal file and
    # write a unix socket file to the same location, we lose the hard uniqueness
    # guarantees of Tempfile. This is OK for this use case, we only generate one
    # Tempfile with this prefix.
    tmp = Tempfile.new("inst-jobs-#{Process.pid}-")
    path = tmp.path
    tmp.close!
    path
  end

  def server(parent_pid: nil)
    # The unix_server_socket method takes care of cleaning up any existing
    # socket for us if the work queue process dies and is restarted.
    listen_socket = Socket.unix_server_socket(@path)
    Server.new(listen_socket, parent_pid: parent_pid)
  end

  def client
    Client.new(Addrinfo.unix(@path))
  end

  class Client
    attr_reader :addrinfo

    def initialize(addrinfo)
      @addrinfo = addrinfo
    end

    def get_and_lock_next_available(worker_name, worker_config)
      @socket ||= @addrinfo.connect
      Marshal.dump([worker_name, worker_config], @socket)
      response = Marshal.load(@socket)
      unless response.nil? || (response.is_a?(Delayed::Job) && response.locked_by == worker_name)
        raise(ProtocolError, "response is not a locked job: #{response.inspect}")
      end
      response
    rescue SystemCallError, IOError
      # The work queue process died. Return nil to signal the worker
      # process should sleep as if no job was found, and then retry.
      @socket = nil
      nil
    end
  end

  class Server
    attr_reader :listen_socket

    def initialize(listen_socket, parent_pid: nil)
      @listen_socket = listen_socket
      @parent_pid = parent_pid
      @clients = {}
      @waiting_clients = {}
    end

    def connected_clients
      @clients.size
    end

    def all_workers_idle?
      !@clients.any? { |_, c| c.working }
    end

    def say(msg, level = :debug)
      if defined?(Rails.logger) && Rails.logger
        Rails.logger.send(level, "[#{Process.pid}]Q #{msg}")
      else
        puts(msg)
      end
    end

    # run the server queue worker
    # this method does not return, only exits or raises an exception
    def run
      say "Starting work queue process"

      while !exit?
        run_once
      end

    rescue => e
      say "WorkQueue Server died: #{e.inspect}"
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

    rescue SystemCallError, IOError, Timeout::Error
      drop_socket(socket)
    end

    def check_for_work
      @waiting_clients.each do |(worker_config, workers)|
        next if workers.empty?

        Delayed::Worker.lifecycle.run_callbacks(:work_queue_pop, self, worker_config) do
          response = Delayed::Job.get_and_lock_next_available(
              workers.map(&:name),
              worker_config[:queue],
              worker_config[:min_priority],
              worker_config[:max_priority])
          response.each do |(worker_name, job)|
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
      Timeout.timeout(Settings.parent_process_client_timeout) { yield }
    end

    ClientState = Struct.new(:working, :socket, :name)
  end
end
end
end
