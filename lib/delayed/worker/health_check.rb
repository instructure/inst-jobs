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
        end

        def build(type:, worker_name:, config: {})
          type = type.to_sym
          klass = @subclasses.find { |sc| sc.type_name == type }
          raise ArgumentError, "Unable to build a HealthCheck for type #{type}" unless klass
          klass.new(worker_name: worker_name, config: config)
        end

        def reschedule_abandoned_jobs
          return if Settings.worker_health_check_type == :none
          Delayed::Job.transaction do
            # this action is a special case, and SHOULD NOT be a periodic job
            # because if it gets wiped out suddenly during execution
            # it can't go clean up it's abandoned self.  Therefore,
            # we expect it to get run from it's own process forked from the job pool
            # and we try to get an advisory lock when it runs.  If we succeed,
            # no other worker is trying to do this right now (and if we abandon the
            # operation, the transaction will end, releasing the advisory lock).
            result = attempt_advisory_lock
            return unless result
            checker = Worker::HealthCheck.build(
              type: Settings.worker_health_check_type,
              config: Settings.worker_health_check_config,
              worker_name: 'cleanup-crew'
            )
            live_workers = checker.live_workers

            Delayed::Job.running_jobs.each do |job|
              # prefetched jobs have their own way of automatically unlocking themselves
              next if job.locked_by.start_with?("prefetch:")
              unless live_workers.include?(job.locked_by)
                begin
                  Delayed::Job.transaction do
                    # double check that the job is still there. locked_by will immediately be reset
                    # to nil in this transaction by Job#reschedule
                    next unless Delayed::Job.where(id: job, locked_by: job.locked_by).update_all(locked_by: "abandoned job cleanup") == 1
                    job.reschedule
                  end
                rescue
                  ::Rails.logger.error "Failure rescheduling abandoned job #{job.id} #{$!.inspect}"
                end
              end
            end
          end
        end

        def attempt_advisory_lock
          lock_name = "Delayed::Worker::HealthCheck#reschedule_abandoned_jobs"
          conn = ActiveRecord::Base.connection
          conn.select_value("SELECT pg_try_advisory_xact_lock(#{conn.quote_table_name('half_md5_as_bigint')}('#{lock_name}'));")
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
