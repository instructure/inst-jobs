require 'delayed/daemon/child'

module Delayed
module Watcher
  mattr_accessor :exiting

  def self.run(child)
    return if child.skip?
    case child
    when ChildProcess
      child.parent_pid = Process.pid
      pid = fork_with_reconnects { child.run }
      child.pid = pid
      processes[pid] = child
    when ChildThread
      thread = Thread.new { child.run }
      child.thread = thread
      threads << [thread, child]
    else
      raise(ArgumentError, "Invalid child class #{child.class.name}")
    end
  end

  def self.on_fork
    processes.clear
    threads.clear
  end

  def self.processes
    @processes ||= {}
  end

  def self.threads
    @threads ||= []
  end

  # blocks until all children have voluntarily quit,
  # or exiting is set
  def self.join(&cb)
    child_died = ->(child) {
      child.exited()
    }

    until exiting || (processes.empty? && threads.empty?)
      unless processes.empty?
        pid = Process.wait(-1, Process::WNOHANG)
        if pid
          child_died.(processes.delete(pid))
        end
      end

      @threads, dead_threads = threads.partition { |(t,_c)| t.status }
      dead_threads.each do |(_t, child)|
        child_died.(child)
      end

      cb.() if cb
      sleep(1)
    end
  end

  # child processes need to reconnect so they don't accidentally share redis or
  # db connections with the parent
  def self.fork_with_reconnects
    fork do
      self.on_fork()
      Pool.on_fork.()
      Delayed::Job.reconnect!
      yield
    end
  end
end
end
