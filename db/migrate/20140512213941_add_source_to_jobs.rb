class AddSourceToJobs < ActiveRecord::Migration
  def connection
    Delayed::Job.connection
  end

  def up
    add_column :delayed_jobs, :source, :string
    add_column :failed_jobs, :source, :string
  end

  def down
    remove_column :delayed_jobs, :source
    remove_column :failed_jobs, :source
  end
end
