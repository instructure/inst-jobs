# frozen_string_literal: true

require "date"

module Delayed
  module Logging
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%6N"
    private_constant :TIMESTAMP_FORMAT

    FORMAT = "%s - %s"
    private_constant :FORMAT

    def self.logger
      return @logger if @logger

      @logger = if defined?(Rails.logger) && Rails.logger
                  Rails.logger
                else
                  ::Logger.new($stdout).tap do |logger|
                    logger.formatter = lambda { |_, time, _, msg|
                      format(FORMAT, time.strftime(TIMESTAMP_FORMAT), msg)
                    }
                  end
                end
    end

    delegate :logger, to: :"Delayed::Logging"

    def say(message, level = :debug)
      logger.send(level, message)
    end
  end
end
