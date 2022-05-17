# frozen_string_literal: true

class AddRequeuedJobIdToFailedJobs < ActiveRecord::Migration[6.0]
  def change
    add_column :failed_jobs, :requeued_job_id, :integer, limit: 8
  end
end
