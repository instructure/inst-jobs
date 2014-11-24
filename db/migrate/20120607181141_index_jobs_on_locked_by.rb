class IndexJobsOnLockedBy < ActiveRecord::Migration
  disable_ddl_transaction! if respond_to?(:disable_ddl_transaction!)

  def connection
    Delayed::Backend::ActiveRecord::Job.connection
  end

  def up
    add_index :delayed_jobs, :locked_by, :algorithm => :concurrently, :where => "locked_by IS NOT NULL"
  end

  def down
    remove_index :delayed_jobs, :locked_by
  end
end
