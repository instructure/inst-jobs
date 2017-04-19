module Delayed
module WorkQueue
class ParentProcess
  class Client
    attr_reader :addrinfo

    include Delayed::Logging

    def initialize(addrinfo)
      @addrinfo = addrinfo
    end

    def get_and_lock_next_available(worker_name, worker_config)
      @socket ||= @addrinfo.connect
      logger.debug("Requesting work using #{@socket.inspect}")
      Marshal.dump([worker_name, worker_config], @socket)
      response = Marshal.load(@socket)
      unless response.nil? || (response.is_a?(Delayed::Job) && response.locked_by == worker_name)
        logger.debug("Received invalid response from server: #{response.inspect}")
        raise(ProtocolError, "response is not a locked job: #{response.inspect}")
      end
      logger.debug("Received work from server: #{response.inspect}")
      response
    rescue SystemCallError, IOError => ex
      logger.error("Work queue connection lost, reestablishing on next poll. (#{ex})")
      # The work queue process died. Return nil to signal the worker
      # process should sleep as if no job was found, and then retry.
      @socket = nil
      nil
    end
  end
end
end
end
