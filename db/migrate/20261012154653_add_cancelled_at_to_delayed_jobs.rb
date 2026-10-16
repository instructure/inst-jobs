# frozen_string_literal: true

class AddCancelledAtToDelayedJobs < ActiveRecord::Migration[7.1]
  def change
    add_column :delayed_jobs, :cancelled_at, :datetime
    add_column :failed_jobs, :cancelled_at, :datetime
  end
end
