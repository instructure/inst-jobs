module Delayed
class Child
  def run
    say("Starting #{self.inspect}")
    perform
  rescue => e
    say("Failed: #{e.inspect}\n#{e.backtrace.join("\n")}", :error)
  end

  def say(msg, level = :debug)
    msg = "[#{Process.pid}] #{self.class.name} #{msg}"
    if defined?(Rails.logger) && Rails.logger
      Rails.logger.send(level, msg)
    else
      puts(msg)
    end
  end

  def exited
  end

  def skip?
    false
  end
end

class ChildProcess < Child
  attr_accessor :pid, :parent_pid

  def run
    $0 = "delayed_jobs_pool#{Settings.pool_procname_suffix}:#{process_name}"
    trap(:INT, "IGNORE")
    super
  end

  def process_name
    self.class.name
  end

  def parent_exited?
    parent_pid != Process.ppid
  end
end

class ChildThread < Child
  attr_accessor :thread

  def join
    @thread.join
  end
end
end
