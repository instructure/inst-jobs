require 'delayed/daemon/child'

module Delayed
module Watcher
  def self.run(child)
    return if child.skip?
    case child
    when ChildProcess
      pid = fork_with_reconnects { child.run }
      child.pid = pid
      processes[pid] = child
    when ChildThread
      thread = Thread.new { child.run }
      threads << thread
    else
      raise(ArgumentError, "Invalid child class #{child.class.name}")
    end
  end

  def self.processes
    @processes ||= {}
  end

  def self.threads
    @threads ||= []
  end

  # blocks until all children have voluntarily quit
  def self.join
    until processes.empty?
      pid = Process.wait
      if pid
        child = processes.delete(pid)
        child.exited()
      end
    end
  end

  # child processes need to reconnect so they don't accidentally share redis or
  # db connections with the parent
  def self.fork_with_reconnects
    fork do
      Pool.on_fork.()
      Delayed::Job.reconnect!
      yield
    end
  end
end
end
