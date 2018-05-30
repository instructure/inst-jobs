module Delayed
  module Backend
    class DeserializationError < StandardError
    end

    class RecordNotFound < DeserializationError
    end

    class JobExpired < StandardError
    end

    module Base
      ON_HOLD_LOCKED_BY = 'on hold'
      ON_HOLD_COUNT = 50

      def self.included(base)
        base.extend ClassMethods
        base.default_priority = Delayed::NORMAL_PRIORITY
        base.before_save :initialize_defaults
      end

      module ClassMethods
        attr_accessor :batches
        attr_accessor :batch_enqueue_args
        attr_accessor :default_priority

        # Add a job to the queue
        # The first argument should be an object that respond_to?(:perform)
        # The rest should be named arguments, these keys are expected:
        # :priority, :run_at, :queue, :strand, :singleton
        # Example: Delayed::Job.enqueue(object, :priority => 0, :run_at => time, :queue => queue)
        def enqueue(*args)
          object = args.shift
          unless object.respond_to?(:perform)
            raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
          end

          options = Settings.default_job_options.merge(args.first || {})
          options[:priority] ||= self.default_priority
          options[:payload_object] = object
          options[:queue] = Delayed::Settings.queue unless options.key?(:queue)
          options[:max_attempts] ||= Delayed::Settings.max_attempts
          options[:source] = Marginalia::Comment.construct_comment if defined?(Marginalia) && Marginalia::Comment.components

          # If two parameters are given to n_strand, the first param is used
          # as the strand name for looking up the Setting, while the second
          # param is appended to make a unique set of strands.
          #
          # For instance, you can pass ["my_job_type", # root_account.global_id]
          # to get a set of n strands per root account, and you can apply the
          # same default to all.
          if options[:n_strand]
            strand_name, ext = options.delete(:n_strand)

            if ext
              full_strand_name = "#{strand_name}/#{ext}"
              num_strands = Delayed::Settings.num_strands.call(full_strand_name)
            else
              full_strand_name = strand_name
            end

            num_strands ||= Delayed::Settings.num_strands.call(strand_name)
            num_strands = num_strands ? num_strands.to_i : 1

            options.merge!(n_strand_options(full_strand_name, num_strands))
          end

          if options[:singleton]
            options[:strand] = options.delete :singleton
            job = self.create_singleton(options)
          elsif batches && options.slice(:strand, :run_at).empty?
            batch_enqueue_args = options.slice(*self.batch_enqueue_args)
            batches[batch_enqueue_args] << options
            return true
          else
            job = self.create(options)
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
          {:strand => strand_name}
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
            raise(ArgumentError, "min_priority #{min_priority} can't be less than #{Delayed::MIN_PRIORITY}")
          end
          if max_priority && max_priority > Delayed::MAX_PRIORITY
            raise(ArgumentError, "max_priority #{max_priority} can't be greater than #{Delayed::MAX_PRIORITY}")
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
          running_jobs.select{|job| job.locked_by.start_with?("#{name}:")}.map{|job| job.locked_by.split(':').last.to_i}
        end

        def unlock_orphaned_prefetched_jobs
          horizon = db_time_now - Settings.parent_process[:prefetched_jobs_timeout] * 4
          orphaned_jobs = running_jobs.select { |job| job.locked_by.start_with?('prefetch:') && job.locked_at < horizon }
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
          running = false if pid
          self.running_jobs.each do |job|
            next unless job.locked_by =~ regex
            unless pid
              job_pid = $1.to_i
              running = Process.kill(0, job_pid) rescue false
            end
            if !running
              unlocked_jobs += 1
              job.reschedule("process died")
            end
          end
          unlocked_jobs
        end
      end

      def failed?
        failed_at
      end
      alias_method :failed, :failed?

      def expired?
        expires_at && (self.class.db_time_now >= expires_at)
      end

      # Reschedule the job in the future (when a job fails).
      # Uses an exponential scale depending on the number of failed attempts.
      def reschedule(error = nil, time = nil)
        begin
          obj = payload_object
          return_code = obj.on_failure(error) if obj && obj.respond_to?(:on_failure)
        rescue
          # don't allow a failed deserialization to prevent rescheduling
        end

        self.attempts += 1 unless return_code == :unlock

        if self.attempts >= (self.max_attempts || Delayed::Settings.max_attempts)
          permanent_failure error || "max attempts reached"
        elsif expired?
          permanent_failure error || "job has expired"
        else
          time ||= self.reschedule_at
          self.run_at = time
          self.unlock
          self.save!
        end
      end

      def permanent_failure(error)
        begin
          # notify the payload_object of a permanent failure
          obj = payload_object
          obj.on_permanent_failure(error) if obj && obj.respond_to?(:on_permanent_failure)
        rescue
          # don't allow a failed deserialization to prevent destroying the job
        end

        # optionally destroy the object
        destroy_self = true
        if Delayed::Worker.on_max_failures
          destroy_self = Delayed::Worker.on_max_failures.call(self, error)
        end

        if destroy_self
          self.destroy
        else
          self.fail!
        end
      end

      def payload_object
        @payload_object ||= deserialize(self['handler'].untaint)
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
        if obj && obj.respond_to?(:full_name)
          obj.full_name
        else
          name
        end
      end

      def payload_object=(object)
        @payload_object = object
        self['handler'] = object.to_yaml
        self['tag'] = if object.respond_to?(:tag)
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
            ::ActiveRecord::Base.clear_active_connections! unless Rails.env.test?
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
        !!(self.locked_at || self.locked_by)
      end

      def reschedule_at
        new_time = self.class.db_time_now + (attempts ** 4) + 5
        begin
          if payload_object.respond_to?(:reschedule_at)
            new_time = payload_object.reschedule_at(
                                        self.class.db_time_now, attempts)
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
        self.save!
      end

      def unhold!
        self.locked_by = nil
        self.locked_at = nil
        self.attempts = 0
        self.run_at = [self.class.db_time_now, self.run_at].max
        self.failed_at = nil
        self.save!
      end

      def on_hold?
        self.locked_by == 'on hold' && self.locked_at && self.attempts == ON_HOLD_COUNT
      end

    private

      ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

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
          'Job failed to load: Unknown handler. Try to manually require the appropriate file.'
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
        if md = ParseObjectFromYaml.match(source)
          md[1].constantize
        end
      end

    public
      def initialize_defaults
        self.queue ||= Delayed::Settings.queue
        self.run_at ||= self.class.db_time_now
      end
    end
  end
end
