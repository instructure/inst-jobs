# frozen_string_literal: true

module Delayed
  class Worker
    class NullHealthCheck < HealthCheck
      self.type_name = :none

      def start # rubocop:disable Naming/PredicateMethod
        true
      end

      def stop # rubocop:disable Naming/PredicateMethod
        true
      end

      def live_workers
        []
      end
    end
  end
end
