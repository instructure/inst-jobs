# frozen_string_literal: true

module Delayed
  class Worker
    class HealthCheck
      @subclasses = []

      class << self
        attr_accessor :type_name
        attr_reader :subclasses

        def inherited(subclass)
          @subclasses << subclass
          super
        end

        def build(type:, worker_name:, config: {})
          type = type.to_sym
          klass = @subclasses.find { |sc| sc.type_name == type }
          raise ArgumentError, "Unable to build a HealthCheck for type #{type}" unless klass

          klass.new(worker_name:, config:)
        end

        def reschedule_abandoned_jobs
          return if Settings.worker_health_check_type == :none

          Delayed::Job.transaction do
            # this action is a special case, and SHOULD NOT be a periodic job
            # because if it gets wiped out suddenly during execution
            # it can't go clean up its abandoned self.  Therefore,
            # we expect it to get run from it's own process forked from the job pool
            # and we try to get an advisory lock when it runs.  If we succeed,
            # no other worker is trying to do this right now (and if we abandon the
            # operation, the transaction will end, releasing the advisory lock).
            result = Delayed::Job.attempt_advisory_lock("Delayed::Worker::HealthCheck#reschedule_abandoned_jobs")
            next unless result

            horizon = 5.minutes.ago

            checker = Worker::HealthCheck.build(
              type: Settings.worker_health_check_type,
              config: Settings.worker_health_check_config,
              worker_name: "cleanup-crew"
            )
            live_workers = checker.live_workers

            loop do
              batch = Delayed::Job.running_jobs
                                  .where("locked_at<?", horizon)
                                  .where.not("locked_by LIKE 'prefetch:%'")
                                  .where.not(locked_by: live_workers)
                                  .limit(100)
                                  .to_a
              break if batch.empty?

              batch.each do |job|
                Delayed::Job.transaction do
                  # double check that the job is still there. locked_by will immediately be reset
                  # to nil in this transaction by Job#reschedule
                  next unless Delayed::Job.where(id: job,
                                                 locked_by: job.locked_by)
                                          .update_all(locked_by: "abandoned job cleanup") == 1

                  job.reschedule
                end
              end
            rescue
              ::Rails.logger.error "Failure rescheduling abandoned job #{job.id} #{$!.inspect}"
            end
          end
        end
      end

      attr_accessor :config, :worker_name

      def initialize(worker_name:, config: {})
        @config = config.with_indifferent_access
        @worker_name = worker_name
      end

      def start
        raise NotImplementedError
      end

      def stop
        raise NotImplementedError
      end

      def live_workers
        raise NotImplementedError
      end
    end
  end
end
