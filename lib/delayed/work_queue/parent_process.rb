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

    def get_and_lock_next_available(name, queue_name, min_priority, max_priority)
      @socket ||= @addrinfo.connect
      Marshal.dump([name, queue_name, min_priority, max_priority], @socket)
      response = Marshal.load(@socket)
      unless response.nil? || (response.is_a?(Delayed::Job) && response.locked_by == name)
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
      readable, _, _ = IO.select(handles, nil, nil, 1)
      if readable
        readable.each { |s| handle_read(s) }
      end
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
      client, _addr = @listen_socket.accept_nonblock
      if client
        @clients[client] = ClientState.new(false)
      end
    rescue IO::WaitReadable
      # ignore and just try accepting again next time through the loop
    end

    def handle_request(socket)
      # There is an assumption here that the client will never send a partial
      # request and then leave the socket open. Doing so would leave us hanging
      # here forever. This is only a reasonable assumption because we control
      # the client.
      request = client_timeout { Marshal.load(socket) }
      response = nil
      Delayed::Worker.lifecycle.run_callbacks(:work_queue_pop, self) do
        response = Delayed::Job.get_and_lock_next_available(*request)
        @clients[socket].working = !response.nil?
      end
      client_timeout { Marshal.dump(response, socket) }
    rescue SystemCallError, IOError, Timeout::Error
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

    ClientState = Struct.new(:working)
  end
end
end
end
