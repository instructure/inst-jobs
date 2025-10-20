# frozen_string_literal: true

module Delayed
  module Util
    def self.process_running?(pid)
      Process.kill(0, pid)
      true
    rescue Errno::ESRCH
      false
    end
  end
end
