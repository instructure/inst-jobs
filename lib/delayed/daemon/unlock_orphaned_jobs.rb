require 'delayed/daemon/task'

module Delayed
class UnlockOrphanedJobs < Task
  attr_reader :target_pid

  def initialize(target_pid = nil)
    @target_pid = target_pid
    self.description = "delayed#{Settings.pool_procname_suffix}:unlock_orphaned_jobs:#{target_pid || 'all'}"
  end

  def execute
    unlocked_jobs = Delayed::Job.unlock_orphaned_jobs(target_pid, daemon_name)
    say "Unlocked #{unlocked_jobs} orphaned jobs" if unlocked_jobs > 0
  end

  def daemon_name
    Socket.gethostname rescue nil
  end
end
end
