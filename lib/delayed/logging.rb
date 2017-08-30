require 'date'

module Delayed
  module Logging
    TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%6N'.freeze
    private_constant :TIMESTAMP_FORMAT

    FORMAT = '%s - %s'
    private_constant :FORMAT


    def self.logger
      return @logger if @logger
      if defined?(Rails.logger) && Rails.logger
        @logger = Rails.logger
      else
        @logger = ::Logger.new(STDOUT).tap do |logger|
          logger.formatter = ->(_, time, _, msg) {
            FORMAT % [
              time.strftime(TIMESTAMP_FORMAT),
              msg
            ]
          }
        end
      end
    end

    def logger
      Delayed::Logging.logger
    end

    def say(message, level = :debug)
      logger.send(level, message)
    end
  end
end
