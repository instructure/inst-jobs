# frozen_string_literal: true

require "pathname"
require "socket"
require "timeout"

require_relative "parent_process/client"
require_relative "parent_process/server"

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

      DEFAULT_SOCKET_NAME = "inst-jobs.sock"
      private_constant :DEFAULT_SOCKET_NAME

      def initialize(config = Settings.parent_process)
        @config = config
        @server_address = generate_socket_path(config["server_address"])
      end

      def server(parent_pid: nil)
        # The unix_server_socket method takes care of cleaning up any existing
        # socket for us if the work queue process dies and is restarted.
        listen_socket = Socket.unix_server_socket(@server_address)
        Server.new(listen_socket, parent_pid: parent_pid, config: @config)
      end

      def client
        Client.new(Addrinfo.unix(@server_address), config: @config)
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
    end
  end
end
