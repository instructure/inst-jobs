module Delayed
  class Worker
    class NullHealthCheck < HealthCheck
      self.type_name = :none

      attr_reader *%i{start stop}

      def live_workers; []; end
    end
  end
end
