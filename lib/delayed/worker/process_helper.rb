# frozen_string_literal: true

module Delayed
  class Worker
    module ProcessHelper
      STAT_LINUX = "stat --format=%%Y /proc/$WORKER_PID"
      STAT_MAC = "ps -o lstart -p $WORKER_PID"
      STAT = RUBY_PLATFORM.include?("darwin") ? STAT_MAC : STAT_LINUX
      ALIVE_CHECK_LINUX = '[ -d "/proc/$WORKER_PID" ]'
      ALIVE_CHECK_MAC = "ps -p $WORKER_PID > /dev/null"
      ALIVE_CHECK = RUBY_PLATFORM.include?("darwin") ? ALIVE_CHECK_MAC : ALIVE_CHECK_LINUX
      SCRIPT_TEMPLATE = <<~SH.freeze
        WORKER_PID="%{pid}" # an example, filled from ruby when the check is created
        ORIGINAL_MTIME="%{mtime}" # an example, filled from ruby when the check is created

        if #{ALIVE_CHECK}; then
            CURRENT_MTIME=$(#{STAT})

            if [ "$ORIGINAL_MTIME" = "$CURRENT_MTIME" ]; then
                exit 0 # Happy day
            else
                echo "PID still exists but procfs entry has changed, current command:"
                ps -p $WORKER_PID -o 'command='
                exit 1 # Something is wrong, trigger a "warning" state
            fi
        else
            exit 255 # The process is no more, trigger a "critical" state.
        fi
      SH

      def self.mtime(pid)
        if RUBY_PLATFORM.include?("darwin")
          `ps -o lstart -p #{pid}`.sub(/\n$/, "").presence
        else
          File::Stat.new("/proc/#{pid}").mtime.to_i.to_s rescue nil
        end
      end

      def self.check_script(pid, mtime)
        format(SCRIPT_TEMPLATE, { pid:, mtime: })
      end

      def self.process_is_still_running?(pid, mtime)
        system(check_script(pid, mtime))
      end
    end
  end
end
