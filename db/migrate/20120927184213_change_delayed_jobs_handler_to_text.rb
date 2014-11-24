class ChangeDelayedJobsHandlerToText < ActiveRecord::Migration
  def connection
    Delayed::Job.connection
  end

  def up
    change_column :delayed_jobs, :handler, :text
  end

  def down
    change_column :delayed_jobs, :handler, :string, :limit => 500.kilobytes
  end
end
