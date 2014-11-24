class AddDelayedJobsStrand < ActiveRecord::Migration
  def connection
    Delayed::Backend::ActiveRecord::Job.connection
  end

  def up
    add_column :delayed_jobs, :strand, :string
    add_index :delayed_jobs, :strand
  end

  def down
    remove_column :delayed_jobs, :strand
  end
end
