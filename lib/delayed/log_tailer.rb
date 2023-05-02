# frozen_string_literal: true

module Delayed
  class LogTailer
    def run
      if Rails.logger.respond_to?(:log_path)
        log_path = Rails.logger.log_path
      elsif Rails.logger.instance_variable_get(:@logdev).try(:instance_variable_get, "@dev").try(:path)
        log_path = Rails.logger.instance_variable_get(:@logdev).instance_variable_get(:@dev).path
      else
        return
      end
      Rails.logger.auto_flushing = true if Rails.logger.respond_to?(:auto_flushing=)
      Thread.new do
        f = File.open(log_path, "r")
        f.seek(0, IO::SEEK_END)
        loop do
          content = f.read
          content.present? ? $stdout.print(content) : sleep(0.5)
        end
      end
    end
  end
end
