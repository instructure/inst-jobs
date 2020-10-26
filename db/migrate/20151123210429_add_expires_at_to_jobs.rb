# frozen_string_literal: true

class AddExpiresAtToJobs < ActiveRecord::Migration[4.2]
  def connection
    Delayed::Job.connection
  end

  def up
    add_column :delayed_jobs, :expires_at, :datetime
    add_column :failed_jobs, :expires_at, :datetime
  end

  def down
    remove_column :delayed_jobs, :expires_at
    remove_column :failed_jobs, :expires_at
  end
end
