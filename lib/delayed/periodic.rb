# frozen_string_literal: true

require "fugit"

module Delayed
  class Periodic
    attr_reader :name, :cron

    def encode_with(coder)
      coder.scalar("!ruby/Delayed::Periodic", @name)
    end

    cattr_accessor :scheduled, :overrides
    self.scheduled = {}
    self.overrides = {}

    def self.add_overrides(overrides)
      overrides.each_value do |cron_line|
        # throws error if the line is malformed
        Fugit.do_parse_cron(cron_line)
      end
      self.overrides.merge!(overrides)
    end

    def self.cron(job_name, cron_line, job_args = {}, &block)
      raise ArgumentError, "job #{job_name} already scheduled!" if scheduled[job_name]

      cron_line = overrides[job_name] || cron_line
      scheduled[job_name] = new(job_name, cron_line, job_args, block)
    end

    def self.audit_queue
      # we used to queue up a job in a strand here, and perform the audit inside that job
      # however, now that we're using singletons for scheduling periodic jobs,
      # it's fine to just do the audit in-line here without risk of creating duplicates
      Delayed::Job.transaction do
        # for db performance reasons, we only need one process doing this at a time
        # so if we can't get an advisory lock, just abort. we'll try again soon
        next unless Delayed::Job.attempt_advisory_lock("Delayed::Periodic#audit_queue")

        perform_audit!
      end
    end

    # make sure all periodic jobs are scheduled for their next run in the job queue
    # this auditing should run on the strand
    def self.perform_audit!
      scheduled.each_value(&:enqueue)
    end

    def initialize(name, cron_line, job_args, block)
      @name = name
      @cron = Fugit.do_parse_cron(cron_line)
      @job_args = { priority: Delayed::LOW_PRIORITY }.merge(job_args.symbolize_keys)
      @block = block
    end

    def enqueue
      Delayed::Job.enqueue(self, **enqueue_args)
    end

    def enqueue_args
      inferred_args = {
        max_attempts: 1,
        run_at: @cron.next_time(Delayed::Periodic.now).utc.to_time,
        singleton: tag,
        on_conflict: :patient
      }
      @job_args.merge(inferred_args)
    end

    def perform
      @block.call
    ensure
      begin
        enqueue
      rescue
        # double fail! the auditor will have to catch this.
        Rails.logger.error "Failure enqueueing periodic job! #{@name} #{$!.inspect}"
      end
    end

    def tag
      "periodic: #{@name}"
    end
    alias_method :display_name, :tag

    def self.now
      Time.zone.now
    end
  end
end
