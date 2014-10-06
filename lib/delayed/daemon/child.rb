module Delayed
class Child
  def run
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
  attr_accessor :pid

  def run
    $0 = "delayed_jobs_pool#{Settings.pool_procname_suffix}:#{process_name}"
    super
  end

  def process_name
    self.class.name
  end
end

class ChildThread < Child
end
end
