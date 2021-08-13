# frozen_string_literal: true

module Delayed
  class Pool
    include Delayed::Logging

    mattr_accessor :on_fork
    self.on_fork = -> {}

    SIGNALS = %i[INT TERM QUIT].freeze
    POOL_SLEEP_PERIOD = 5

    attr_reader :workers

    def initialize(*args)
      if args.first.is_a?(Hash)
        @config = args.first
      else
        warn "Calling Delayed::Pool.new directly is deprecated. Use `Delayed::CLI.new.run()` instead."
      end
      @workers = {}
      @signal_queue = []
      @self_pipe = IO.pipe
    end

    def run
      warn "Delayed::Pool#run is deprecated and will be removed. Use `Delayed::CLI.new.run()` instead."
      Delayed::CLI.new.run
    end

    def start
      say "Started job master", :info
      SIGNALS.each do |sig|
        trap(sig) do
          @signal_queue << sig
          wake_up
        end
      end
      $0 = procname
      # fork to handle unlocking (to prevent polluting the parent with worker objects)
      unlock_pid = fork_with_reconnects do
        unlock_orphaned_jobs
      end
      Process.wait unlock_pid

      spawn_periodic_auditor
      spawn_abandoned_job_cleanup
      spawn_all_workers
      say "Workers spawned"
      join
      say "Shutting down"
      stop
      reap_all_children
    rescue Exception => e # rubocop:disable Lint/RescueException
      say "Job master died with error: #{e.inspect}\n#{e.backtrace.join("\n")}", :fatal
      raise
    end

    protected

    def procname
      "delayed_jobs_pool#{Settings.pool_procname_suffix}"
    end

    def unlock_orphaned_jobs(_worker = nil, pid = nil)
      return if Settings.disable_automatic_orphan_unlocking

      unlocked_jobs = Delayed::Job.unlock_orphaned_jobs(pid)
      say "Unlocked #{unlocked_jobs} orphaned jobs" if unlocked_jobs.positive?
      ActiveRecord::Base.connection_handler.clear_all_connections! unless Rails.env.test?
    end

    def spawn_all_workers
      ActiveRecord::Base.connection_handler.clear_all_connections!

      if @config[:work_queue] == "parent_process"
        @work_queue = WorkQueue::ParentProcess.new
        spawn_work_queue
      end

      @config[:workers].each do |worker_config|
        (worker_config[:workers] || 1).times { spawn_worker(worker_config) }
      end
    end

    def spawn_work_queue
      parent_pid = Process.pid
      pid = fork_with_reconnects do
        $0 = "delayed_jobs_work_queue#{Settings.pool_procname_suffix}"
        @work_queue.server(parent_pid: parent_pid).run
      end
      workers[pid] = :work_queue
    end

    def spawn_worker(worker_config)
      return if worker_config[:periodic] # backwards compat

      worker_config[:parent_pid] = Process.pid
      worker_config[:work_queue] = @work_queue.client if @work_queue
      worker = Delayed::Worker.new(worker_config)

      pid = fork_with_reconnects do
        worker.start
      end
      workers[pid] = worker
    end

    # child processes need to reconnect so they don't accidentally share redis or
    # db connections with the parent
    def fork_with_reconnects
      fork do
        @self_pipe.each(&:close) # sub-processes don't need to wake us up; keep their FDs clean
        Pool.on_fork.call
        Delayed::Job.reconnect!
        yield
      end
    end

    def spawn_abandoned_job_cleanup
      return if Settings.disable_abandoned_job_cleanup

      cleanup_interval_in_minutes = 60
      @abandoned_cleanup_thread = Thread.new do
        # every hour (staggered by process)
        # check for dead jobs and cull them.
        # Will actually be more often based on the
        # number of worker nodes in the pool.  This will actually
        # be a max of N times per hour where N is the number of workers,
        # but they won't overrun each other because the health check
        # takes an advisory lock internally
        sleep(rand(cleanup_interval_in_minutes * 60))
        loop do
          schedule_abandoned_job_cleanup
          sleep(cleanup_interval_in_minutes * 60)
        end
      end
    end

    def schedule_abandoned_job_cleanup
      pid = fork_with_reconnects do
        # we want to avoid db connections in the main pool process
        $0 = "delayed_abandoned_job_cleanup"
        Delayed::Worker::HealthCheck.reschedule_abandoned_jobs
      end
      workers[pid] = :abandoned_job_cleanup
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
        maintain_children
        case sig = @signal_queue.shift
        when nil
          pool_sleep
        when :QUIT
          break
        when :TERM, :INT
          stop(graceful: false) if Settings.kill_workers_on_exit
          break
        else
          logger.warn("Unexpected signal received: #{sig}")
        end
      end
    end

    def pool_sleep
      IO.select([@self_pipe[0]], nil, nil, POOL_SLEEP_PERIOD)
      @self_pipe[0].read_nonblock(11, exception: false)
    end

    def stop(graceful: true, timeout: Settings.slow_exit_timeout)
      signal_for_children = graceful ? :QUIT : :TERM
      if Settings.kill_workers_on_exit
        limit = Time.now + timeout
        until @workers.empty? || Time.now >= limit
          signal_all_children(signal_for_children)
          # Give our children some time to process the signal before checking if
          # they've exited
          sleep(0.5)
          reap_all_children.each { |pid| @workers.delete(pid) }
        end

        # We really want to give the workers every oportunity to clean up after
        # themselves before murdering them.
        stop(graceful: false, timeout: 2) if graceful
        signal_all_children(:KILL)
      else
        signal_all_children(signal_for_children)
      end
    end

    def signal_all_children(signal)
      workers.each_key { |pid| signal_child(signal, pid) }
    end

    def signal_child(signal, pid)
      Process.kill(signal, pid)
    rescue Erron::ESRCH
      workers.delete(pid)
    end

    # Respawn all children that have exited since we last checked
    def maintain_children
      reap_all_children.each do |pid|
        respawn_child(pid)
      end
    end

    # Reaps processes that have exited or just returns if none have exited
    #
    # @return Array An array of child pids that have exited
    def reap_all_children
      exited_pids = []
      loop do
        pid = Process.wait(-1, Process::WNOHANG)
        break unless pid

        exited_pids << pid
      rescue Errno::ECHILD
        break
      end
      exited_pids
    end

    def respawn_child(child)
      return unless workers.include?(child)

      worker = workers.delete(child)
      case worker
      when :periodic_audit
        say "ran auditor: #{worker}"
      when :abandoned_job_cleanup
        say "ran cleanup: #{worker}"
      when :work_queue
        say "work queue exited, restarting", :info
        spawn_work_queue
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

    def wake_up
      @self_pipe[1].write_nonblock(".", exception: false)
    end
  end
end
