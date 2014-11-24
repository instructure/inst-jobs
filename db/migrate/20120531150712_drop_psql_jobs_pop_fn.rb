class DropPsqlJobsPopFn < ActiveRecord::Migration
  def connection
    Delayed::Backend::ActiveRecord::Job.connection
  end

  def up
    if connection.adapter_name == 'PostgreSQL'
      connection.execute("DROP FUNCTION IF EXISTS pop_from_delayed_jobs(varchar, varchar, integer, integer, timestamp without time zone)")
    end
  end

  def down
    raise ActiveRecord::IrreversibleMigration
  end
end
