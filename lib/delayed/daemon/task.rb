module Delayed
class Task
  attr_accessor :thread, :pid, :parent_pid
  attr_reader :status, :description

  def children
    @children ||= []
  end

  def say(msg, level = :debug)
    msg = "[#{Process.pid}] #{self.class.name} #{msg}"
    if defined?(Rails.logger) && Rails.logger
      Rails.logger.send(level, msg)
    else
      puts(msg)
    end
  end

  def description=(new_desc)
    @description = new_desc
    if @description && status == :running && !@ignore_process_names
      $0 = @description
    end
  end

  def parent_exited?
    if parent_pid
      parent_pid != Process.ppid
    else
      false
    end
  end

  def process?
    !thread
  end

  def run_as_process
    self.parent_pid = Process.pid
    self.pid = fork_with_reconnects { self.run_task }
    self
  end

  def run_as_thread
    @ignore_process_names = true
    self.thread = Thread.new { self.run_task }
    self
  end

  def run_top_level(parent_pid)
    self.parent_pid = parent_pid
    self.pid = Process.pid
    self.run_task
    self
  end

  def join
    if process?
      Process.wait(self.pid)
    else
      thread.join()
    end
  end

  def alive?
    if process?
      !Process.wait(self.pid, Process::WNOHANG)
    else
      !!thread.status
    end
  end

  def child_died(child)
    # no-op
  end

  def run_loop(&cb)
    until @stopping
      children.each do |child|
        if !child.alive?
          children.delete(child)
          child_died(child)
        end
      end

      if cb
        cb.()
      else
        sleep(0.5)
      end

      shutdown! if parent_exited?
    end
  end

  def shutdown!
    @stopping = true
  end

  protected

  def run_task
    @status = :running
    # force initial naming
    self.description = @description

    if process?
      trap(:INT, "IGNORE") # so foreground runs only catch SIGINT in the parent
    end

    say("Starting #{self.inspect}")
    execute
  rescue => e
    say("Failed: #{e.inspect}\n#{e.backtrace.join("\n")}", :error)
  ensure
    @status = :done
  end

  # child processes need to reconnect so they don't accidentally share redis or
  # db connections with the parent
  def fork_with_reconnects
    fork do
      Delayed::Pool.on_fork.()
      Delayed::Job.reconnect!
      yield
    end
  end
end
end
