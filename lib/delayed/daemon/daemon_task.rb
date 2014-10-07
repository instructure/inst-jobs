require 'delayed/daemon/task'

require 'delayed/daemon/periodic_auditor'
require 'delayed/daemon/unlock_orphaned_jobs'
require 'delayed/daemon/worker_tasks'

module Delayed
class DaemonTask < Task
  def initialize(config)
    @config = config
    self.description = "delayed_jobs_pool#{Settings.pool_procname_suffix}"
  end

  def execute
    trap(:INT) { self.shutdown! }
    trap(:TERM) { self.shutdown! }

    say "Started job master", :info
    # We join because we want the unlocker to finish before continuing, or we
    # might unlock our own jobs.
    unless Settings.disable_automatic_orphan_unlocking
      UnlockOrphanedJobs.new.run_as_process.join
    end

    say "Finished unlocking"
    unless Settings.disable_periodic_jobs
      children << PeriodicAuditor.new.run_as_thread
    end

    spawn_all_workers
    run_loop

    say "Shutting down", :info
  end

  def spawn_all_workers
    ActiveRecord::Base.connection_handler.clear_all_connections!

    @config[:workers].each do |worker_config|
      worker_config = worker_config.with_indifferent_access
      next if worker_config[:periodic] # backwards compat
      worker_config = @config.merge(worker_config)
      children << WorkerGroup.new(worker_config).run_as_process
    end
  end

  def child_died(child)
    case child
    when PeriodicAuditor
      children << PeriodicAuditor.new.run_as_thread
    when WorkerGroup
      children << WorkerGroup.new(child.config).run_as_process
    end
  end
end
end
