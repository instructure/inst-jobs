# frozen_string_literal: true

module Delayed
module WorkQueue
class ParentProcess
  class Client
    attr_reader :addrinfo

    include Delayed::Logging

    def initialize(addrinfo, config: Settings.parent_process)
      @addrinfo = addrinfo
      @connect_timeout = config['client_connect_timeout'] || 2
    end

    def init
      @self_pipe ||= IO.pipe
    end

    def close
      reset_connection
    end

    def get_and_lock_next_available(worker_name, worker_config)
      Marshal.dump([worker_name, worker_config], socket)

      # We're assuming there won't ever be a partial write here so we only need
      # to wait for anything to be available on the 'wire', this is a valid
      # assumption because we control the server and it's a Unix domain socket,
      # not TCP.
      if socket.eof?
        # Other end closed gracefully, so should we
        logger.debug("server closed connection")
        return reset_connection
      end

      readers, _, _ = IO.select([socket, @self_pipe[0]])

      if readers.include?(@self_pipe[0])
        # we're probably exiting so we just want to break out of the blocking read
        logger.debug("Broke out of select due to being awakened, exiting")
      else
        Marshal.load(socket).tap do |response|
          unless response.nil? || (response.is_a?(Delayed::Job) && response.locked_by == worker_name)
            raise(ProtocolError, "response is not a locked job: #{response.inspect}")
          end
          logger.debug("Received job #{response.id}")
        end
      end
    rescue SystemCallError, IOError => ex
      logger.error("Work queue connection lost, reestablishing on next poll. (#{ex})")
      # The work queue process died. Return nil to signal the worker
      # process should sleep as if no job was found, and then retry.
      reset_connection
    end

    def wake_up
      @self_pipe[1].write_nonblock('.', exception: false)
    end

    private

    def socket
      @socket ||= @addrinfo.connect(timeout: @connect_timeout)
    end

    def reset_connection
      if @socket
        @socket.close
        @socket = nil
      end
    end
  end
end
end
end
