class AddDelayedJobsMaxAttempts < ActiveRecord::Migration
  def connection
    Delayed::Backend::ActiveRecord::Job.connection
  end

  def up
    add_column :delayed_jobs, :max_attempts, :integer
  end

  def down
    remove_column :delayed_jobs, :max_attempts
  end
end
