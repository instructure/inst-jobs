module Delayed
class Pool
  mattr_accessor :on_fork
  self.on_fork = ->{ }

  attr_reader :workers

  def initialize(*args)
    if args.size == 1 && args.first.is_a?(Array)
      worker_configs = args.first
    else
      warn "Calling Delayed::Pool.new directly is deprecated. Use `Delayed::CLI.new.run()` instead."
    end
    @workers = {}
    @config = { workers: worker_configs }
  end

  def run
    warn "Delayed::Pool#run is deprecated and will be removed. Use `Delayed::CLI.new.run()` instead."
    Delayed::CLI.new.run()
  end

  def start
    say "Started job master", :info
    $0 = procname
    # fork to handle unlocking (to prevent polluting the parent with worker objects)
    unlock_pid = fork_with_reconnects do
      unlock_orphaned_jobs
    end
    Process.wait unlock_pid

    spawn_periodic_auditor
    spawn_all_workers
    say "Workers spawned"
    join
    say "Shutting down"
  rescue Interrupt => e
    say "Signal received, exiting", :info
  rescue Exception => e
    say "Job master died with error: #{e.inspect}\n#{e.backtrace.join("\n")}", :fatal
    raise
  end

  protected

  def procname
    "delayed_jobs_pool#{Settings.pool_procname_suffix}"
  end

  def say(msg, level = :debug)
    if defined?(Rails.logger) && Rails.logger
      Rails.logger.send(level, "[#{Process.pid}]P #{msg}")
    else
      puts(msg)
    end
  end

  def unlock_orphaned_jobs(worker = nil, pid = nil)
    return if Settings.disable_automatic_orphan_unlocking

    unlocked_jobs = Delayed::Job.unlock_orphaned_jobs(pid)
    say "Unlocked #{unlocked_jobs} orphaned jobs" if unlocked_jobs > 0
    ActiveRecord::Base.connection_handler.clear_all_connections! unless Rails.env.test?
  end

  def spawn_all_workers
    ActiveRecord::Base.connection_handler.clear_all_connections!

    @config[:workers].each do |worker_config|
      (worker_config[:workers] || 1).times { spawn_worker(worker_config) }
    end
  end

  def spawn_worker(worker_config)
    if worker_config[:periodic]
      return # backwards compat
    else
      worker_config[:parent_pid] = Process.pid
      worker = Delayed::Worker.new(worker_config)
    end

    pid = fork_with_reconnects do
      worker.start
    end
    workers[pid] = worker
  end

  # child processes need to reconnect so they don't accidentally share redis or
  # db connections with the parent
  def fork_with_reconnects
    fork do
      Pool.on_fork.()
      Delayed::Job.reconnect!
      yield
    end
  end

  def spawn_periodic_auditor
    return if Settings.disable_periodic_jobs

    @periodic_thread = Thread.new do
      # schedule the initial audit immediately on startup
      schedule_periodic_audit
      # initial sleep is randomized, for some staggering in the audit calls
      # since job processors are usually all restarted at the same time
      sleep(rand(15 * 60))
      loop do
        schedule_periodic_audit
        sleep(15 * 60)
      end
    end
  end

  def schedule_periodic_audit
    pid = fork_with_reconnects do
      # we want to avoid db connections in the main pool process
      $0 = "delayed_periodic_audit_scheduler"
      Delayed::Periodic.audit_queue
    end
    workers[pid] = :periodic_audit
  end

  def join
    loop do
      child = Process.wait
      if workers.include?(child)
        worker = workers.delete(child)
        if worker.is_a?(Symbol)
          say "ran auditor: #{worker}"
        else
          say "child exited: #{child}, restarting", :info
          # fork to handle unlocking (to prevent polluting the parent with worker objects)
          unlock_pid = fork_with_reconnects do
            unlock_orphaned_jobs(worker, child)
          end
          Process.wait unlock_pid
          spawn_worker(worker.config)
        end
      end
    end
  end
end
end
