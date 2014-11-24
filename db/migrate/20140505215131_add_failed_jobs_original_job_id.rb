class AddFailedJobsOriginalJobId < ActiveRecord::Migration
  def connection
    Delayed::Backend::ActiveRecord::Job.connection
  end

  def up
    add_column :failed_jobs, :original_job_id, :integer, limit: 8
  end

  def down
    remove_column :failed_jobs, :original_job_id
  end
end
