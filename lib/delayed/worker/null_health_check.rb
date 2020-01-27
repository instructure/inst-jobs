module Delayed
  class Worker
    class NullHealthCheck < HealthCheck
      self.type_name = :none

      def start
        true
      end

      def stop
        true
      end

      def live_workers; []; end
    end
  end
end
