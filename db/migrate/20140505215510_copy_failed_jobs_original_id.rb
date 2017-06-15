class CopyFailedJobsOriginalId < ActiveRecord::Migration[4.2]
  def connection
    Delayed::Backend::ActiveRecord::Job.connection
  end

  def up
    # this is a smaller, less frequently accessed table, so we just update all at once
    Delayed::Backend::ActiveRecord::Job::Failed.where("original_job_id is null").update_all("original_job_id = original_id")
  end

  def down
  end
end
