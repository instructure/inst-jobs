require 'delayed/daemon/task'

module Delayed
class PeriodicAuditor < Task
  def execute
    # schedule the initial audit immediately on startup
    schedule_periodic_audit
    # initial sleep is randomized, for some staggering in the audit calls
    # since job processors are usually all restarted at the same time
    sleep(rand(Settings.periodic_jobs_audit_frequency))
    loop do
      schedule_periodic_audit
      sleep(Settings.periodic_jobs_audit_frequency)
    end
  end

  private

  def schedule_periodic_audit
    # We only run this in a child process to avoid doing real db work in the
    # daemon process.
    PeriodicAuditQueuer.new.run_as_process.join
  end
end

class PeriodicAuditQueuer < Task
  def initialize
    self.description = "delayed#{Settings.pool_procname_suffix}:periodic_audit_queuer"
  end

  def execute
    Delayed::Periodic.audit_queue
  end

  def exited
    say "ran periodic audit"
  end
end
end
