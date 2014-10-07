require 'delayed/daemon/task'

module Delayed
class WorkerGroup < Task
  def initialize(config)
    @config = config.dup
    self.description = process_name
  end

  def execute
    trap(:TERM) { self.shutdown! }
    if @config[:workers] > 1
      @config[:workers].times { children << WorkerProcess.new(@config).run_as_process }
      run_loop
    else
      WorkerProcess.new(@config).run_top_level(self.parent_pid)
    end
  end

  def child_died(child)
    say "child exited: #{child.inspect}, restarting", :info
    children << WorkerProcess.new(@config).run_as_process
  end

  def process_name
    max_priority = @config[:max_priority] == Delayed::MAX_PRIORITY ? 'max' : @config[:max_priority]
    "delayed#{Settings.pool_procname_suffix}:worker_group:#{Settings.worker_procname_prefix}#{@config[:queue]}:#{@config[:min_priority]}:#{max_priority}:#{@config[:workers]}:#{@config[:threads_per_process]}"
  end
end

class WorkerProcess < Task
  def initialize(config)
    @config = config.dup
    self.description = process_name
  end

  def execute
    trap(:TERM) { self.shutdown! }

    if @config[:threads_per_process] == 1
      new_worker("").run_top_level(self.parent_pid)
    else
      @config[:threads_per_process].times { |i|
        children << new_worker(":#{i}").run_as_thread
      }

      run_loop

      children.each { |wt| wt.shutdown! }
      children.each { |wt| wt.join }
    end
  end

  def new_worker(id)
    worker_name = "#{Socket.gethostname rescue "X"}:#{Process.pid}#{id}"
    Worker.new(@config.merge(worker_name: worker_name))
  end

  def child_died(child)
    say "child exited: #{child.inspect}, restarting", :info
    # TODO: unlock with per-thread names
    #UnlockOrphanedJobs.new(self.pid)
    children << Worker.new(child.config).run_as_thread
  end

  def process_name
    max_priority = @config[:max_priority] == Delayed::MAX_PRIORITY ? 'max' : @config[:max_priority]
    "delayed#{Settings.pool_procname_suffix}:worker_proc:#{Settings.worker_procname_prefix}#{@config[:queue]}:#{@config[:min_priority]}:#{max_priority}:#{@config[:workers]}:#{@config[:threads_per_process]}"
  end
end
end
