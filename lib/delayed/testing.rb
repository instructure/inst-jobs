# frozen_string_literal: true

module Delayed
module Testing
  def self.run_job(job)
    Delayed::Worker.new.perform(job)
  end

  def self.drain
    while job = Delayed::Job.get_and_lock_next_available(
        'spec run_jobs',
        Delayed::Settings.queue,
        0,
        Delayed::MAX_PRIORITY)
      run_job(job)
    end
  end

  def self.track_created
    job_tracking = JobTracking.track { yield }
    job_tracking.created
  end

  def self.clear_all!
    case Delayed::Job.name
    when /Redis/
      Delayed::Job.redis.flushdb
    when /ActiveRecord/
      Delayed::Job.delete_all
      Delayed::Job::Failed.delete_all
    end
  end
end
end
