# frozen_string_literal: true

module Delayed
  module Backend
    class DeserializationError < StandardError
    end

    class RecordNotFound < DeserializationError
    end

    class JobExpired < StandardError
    end

    module Base
      ON_HOLD_BLOCKER = "blocker job"
      ON_HOLD_LOCKED_BY = "on hold"
      ON_HOLD_COUNT = 50

      def self.included(base)
        base.extend ClassMethods
        base.default_priority = Delayed::NORMAL_PRIORITY
        base.before_save :initialize_defaults
      end

      module ClassMethods
        attr_accessor :batches, :batch_enqueue_args, :default_priority

        # Add a job to the queue
        # The first argument should be an object that respond_to?(:perform)
        # The rest should be named arguments, these keys are expected:
        # :priority, :run_at, :queue, :strand, :singleton
        # Example: Delayed::Job.enqueue(object, priority: 0, run_at: time, queue: queue)
        def enqueue(object,
                    priority: default_priority,
                    run_at: nil,
                    expires_at: nil,
                    queue: Delayed::Settings.queue,
                    strand: nil,
                    singleton: nil,
                    n_strand: nil,
                    max_attempts: Delayed::Settings.max_attempts,
                    **kwargs)

          unless object.respond_to?(:perform)
            raise ArgumentError, "Cannot enqueue items which do not respond to perform"
          end

          strand ||= singleton if Settings.infer_strand_from_singleton

          kwargs = Settings.default_job_options.merge(kwargs)
          kwargs[:payload_object] = object
          kwargs[:priority] = priority
          kwargs[:run_at] = run_at if run_at
          kwargs[:strand] = strand
          kwargs[:max_attempts] = max_attempts
          if defined?(Marginalia) && Marginalia::Comment.components
            kwargs[:source] =
              Marginalia::Comment.construct_comment
          end
          kwargs[:expires_at] = expires_at
          kwargs[:queue] = queue
          kwargs[:singleton] = singleton

          raise ArgumentError, "Only one of strand or n_strand can be used" if strand && n_strand

          # If two parameters are given to n_strand, the first param is used
          # as the strand name for looking up the Setting, while the second
          # param is appended to make a unique set of strands.
          #
          # For instance, you can pass ["my_job_type", # root_account.global_id]
          # to get a set of n strands per root account, and you can apply the
          # same default to all.
          if n_strand
            strand_name, ext = n_strand

            if ext
              full_strand_name = "#{strand_name}/#{ext}"
              num_strands = Delayed::Settings.num_strands.call(full_strand_name)
            else
              full_strand_name = strand_name
            end

            num_strands ||= Delayed::Settings.num_strands.call(strand_name)
            num_strands = num_strands ? num_strands.to_i : 1

            kwargs.merge!(n_strand_options(full_strand_name, num_strands))
          end

          job = nil

          if singleton
            Delayed::Worker.lifecycle.run_callbacks(:create, kwargs) do
              job = create(**kwargs)
            end
          elsif batches && strand.nil? && run_at.nil?
            batch_enqueue_args = kwargs.slice(*self.batch_enqueue_args)
            batches[batch_enqueue_args] << kwargs
            return true
          else
            raise ArgumentError, "on_conflict can only be provided with singleton" if kwargs[:on_conflict]

            Delayed::Worker.lifecycle.run_callbacks(:create, kwargs) do
              job = create(**kwargs)
            end
          end

          JobTracking.job_created(job)

          job
        end

        # by default creates a new strand name randomly based on num_strands
        # effectively balancing the load during queueing
        # overwritten in ActiveRecord::Job to use triggers to balance at run time
        def n_strand_options(strand_name, num_strands)
          strand_num = num_strands > 1 ? rand(num_strands) + 1 : 1
          strand_name += ":#{strand_num}" if strand_num > 1
          { strand: strand_name }
        end

        def in_delayed_job?
          !!Thread.current[:in_delayed_job]
        end

        def in_delayed_job=(val)
          Thread.current[:in_delayed_job] = val
        end

        def check_queue(queue)
          raise(ArgumentError, "queue name can't be blank") if queue.blank?
        end

        def check_priorities(min_priority, max_priority)
          if min_priority && min_priority < Delayed::MIN_PRIORITY
            raise ArgumentError, "min_priority #{min_priority} can't be less than #{Delayed::MIN_PRIORITY}"
          end
          if max_priority && max_priority > Delayed::MAX_PRIORITY # rubocop:disable Style/GuardClause
            raise ArgumentError, "max_priority #{max_priority} can't be greater than #{Delayed::MAX_PRIORITY}"
          end
        end

        # Get the current time (UTC)
        # Note: This does not ping the DB to get the time, so all your clients
        # must have syncronized clocks.
        def db_time_now
          Time.now.utc
        end

        def processes_locked_locally(name: nil)
          name ||= Socket.gethostname rescue x
          local_jobs = running_jobs.select do |job|
            job.locked_by.start_with?("#{name}:")
          end
          local_jobs.map { |job| job.locked_by.split(":").last.to_i }
        end

        def unlock_orphaned_prefetched_jobs
          horizon = db_time_now - (Settings.parent_process[:prefetched_jobs_timeout] * 4)
          orphaned_jobs = running_jobs.select do |job|
            job.locked_by.start_with?("prefetch:") && job.locked_at < horizon
          end
          return 0 if orphaned_jobs.empty?

          unlock(orphaned_jobs)
        end

        def unlock_orphaned_jobs(pid = nil, name = nil)
          begin
            name ||= Socket.gethostname
          rescue
            return 0
          end
          pid_regex = pid || '(\d+)'
          regex = Regexp.new("^#{Regexp.escape(name)}:#{pid_regex}$")
          unlocked_jobs = 0
          escaped_name = name.gsub("\\", "\\\\")
                             .gsub("%", "\\%")
                             .gsub("_", "\\_")
          locked_by_like = "#{escaped_name}:%"
          running = false if pid
          jobs = running_jobs.limit(100)
          jobs = pid ? jobs.where(locked_by: "#{name}:#{pid}") : jobs.where("locked_by LIKE ?", locked_by_like)
          ignores = []
          loop do
            batch_scope = ignores.empty? ? jobs : jobs.where.not(id: ignores)
            # if we don't reload this it's possible to keep getting the
            # same array each loop even after the jobs have been deleted.
            batch = batch_scope.reload.to_a
            break if batch.empty?

            batch.each do |job|
              unless job.locked_by =~ regex
                ignores << job.id
                next
              end

              unless pid
                job_pid = $1.to_i
                running = Process.kill(0, job_pid) rescue false
              end

              if running
                ignores << job.id
              else
                unlocked_jobs += 1
                job.reschedule("process died")
              end
            end
          end
          unlocked_jobs
        end
      end

      def failed?
        failed_at
      end
      alias failed failed?

      def expired?
        expires_at && (self.class.db_time_now >= expires_at)
      end

      def inferred_max_attempts
        max_attempts || Delayed::Settings.max_attempts
      end

      # Reschedule the job in the future (when a job fails).
      # Uses an exponential scale depending on the number of failed attempts.
      def reschedule(error = nil, time = nil)
        begin
          obj = payload_object
          return_code = obj.on_failure(error) if obj.respond_to?(:on_failure)
        rescue
          # don't allow a failed deserialization to prevent rescheduling
        end

        self.attempts += 1 unless return_code == :unlock

        if self.attempts >= inferred_max_attempts
          permanent_failure error || "max attempts reached"
        elsif expired?
          permanent_failure error || "job has expired"
        else
          time ||= reschedule_at
          self.run_at = time
          unlock
          save!
        end
      end

      def permanent_failure(error)
        begin
          # notify the payload_object of a permanent failure
          obj = payload_object
          obj.on_permanent_failure(error) if obj.respond_to?(:on_permanent_failure)
        rescue
          # don't allow a failed deserialization to prevent destroying the job
        end

        # optionally destroy the object
        destroy_self = true
        destroy_self = Delayed::Worker.on_max_failures.call(self, error) if Delayed::Worker.on_max_failures

        if destroy_self
          destroy
        else
          fail!
        end
      end

      def payload_object
        @payload_object ||= deserialize(self["handler"].untaint)
      end

      def name
        @name ||= begin
          payload = payload_object
          if payload.respond_to?(:display_name)
            payload.display_name
          else
            payload.class.name
          end
        end
      end

      def full_name
        obj = payload_object rescue nil
        if obj.respond_to?(:full_name)
          obj.full_name
        else
          name
        end
      end

      def payload_object=(object)
        @payload_object = object
        self["handler"] = object.to_yaml
        self["tag"] = if object.respond_to?(:tag)
                        object.tag
                      elsif object.is_a?(Module)
                        "#{object}.perform"
                      else
                        "#{object.class}#perform"
                      end
      end

      # Moved into its own method so that new_relic can trace it.
      def invoke_job
        Delayed::Worker.lifecycle.run_callbacks(:invoke_job, self) do
          Delayed::Job.in_delayed_job = true
          begin
            payload_object.perform
          ensure
            Delayed::Job.in_delayed_job = false
            unless Rails.env.test?
              if Rails.version < "6.1"
                ::ActiveRecord::Base.clear_active_connections!
              else
                ::ActiveRecord::Base.clear_active_connections!(nil)
              end
            end
          end
        end
      end

      def batch?
        payload_object.is_a?(Delayed::Batch::PerformableBatch)
      end

      # Unlock this job (note: not saved to DB)
      def unlock
        self.locked_at    = nil
        self.locked_by    = nil
      end

      def locked?
        !!(locked_at || locked_by)
      end

      def reschedule_at
        new_time = self.class.db_time_now + (attempts**4) + 5
        begin
          if payload_object.respond_to?(:reschedule_at)
            new_time = payload_object.reschedule_at(
              self.class.db_time_now, attempts
            )
          end
        rescue
          # TODO: just swallow errors from reschedule_at ?
        end
        new_time
      end

      def hold!
        self.locked_by = ON_HOLD_LOCKED_BY
        self.locked_at = self.class.db_time_now
        self.attempts = ON_HOLD_COUNT
        save!
      end

      def unhold!
        self.locked_by = nil
        self.locked_at = nil
        self.attempts = 0
        self.run_at = [self.class.db_time_now, run_at].max
        self.failed_at = nil
        save!
      end

      def on_hold?
        locked_by == "on hold" && locked_at && self.attempts == ON_HOLD_COUNT
      end

      private

      PARSE_OBJECT_FROM_YAML = %r{!ruby/\w+:([^\s]+)}.freeze
      private_constant :PARSE_OBJECT_FROM_YAML

      def deserialize(source)
        handler = nil
        begin
          handler = _yaml_deserialize(source)
        rescue TypeError, ArgumentError
          attempt_to_load_from_source(source)
          handler = _yaml_deserialize(source)
        end

        return handler if handler.respond_to?(:perform)

        raise DeserializationError,
              "Job failed to load: Unknown handler. Try to manually require the appropriate file."
      rescue TypeError, LoadError, NameError => e
        raise DeserializationError,
              "Job failed to load: #{e.message}. Try to manually require the required file."
      rescue Psych::SyntaxError => e
        raise DeserializationError,
              "YAML parsing error: #{e.message}. Probably not recoverable."
      end

      def _yaml_deserialize(source)
        YAML.respond_to?(:unsafe_load) ? YAML.unsafe_load(source) : YAML.load(source)
      end

      def attempt_to_load_from_source(source)
        return unless (md = PARSE_OBJECT_FROM_YAML.match(source))

        md[1].constantize
      end

      public

      def initialize_defaults
        self.queue ||= Delayed::Settings.queue
        self.run_at ||= self.class.db_time_now
      end
    end
  end
end
