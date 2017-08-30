require 'fileutils'

module Delayed
# Daemon controls the parent proces that runs the Pool and monitors the Worker processes.
class Daemon
  attr_reader :pid_folder

  def initialize(pid_folder)
    @pid_folder = pid_folder
  end

  def status(print: true, pid: self.pid)
    alive = pid && (Process.kill(0, pid) rescue false) && :running
    alive ||= :draining if pid && Process.kill(0, -pid) rescue false
    if alive
      puts "Delayed jobs #{alive}, pool PID: #{pid}" if print
    else
      puts "No delayed jobs pool running" if print && print != :alive
    end
    alive
  end

  def daemonize!
    FileUtils.mkdir_p(pid_folder)
    puts "Daemonizing..."

    exit if fork
    Process.setsid
    exit if fork
    Process.setpgrp

    @daemon = true
    lock_file = File.open(pid_file, 'wb')
    # someone else is already running; just exit
    unless lock_file.flock(File::LOCK_EX | File::LOCK_NB)
      exit
    end
    at_exit { lock_file.flock(File::LOCK_UN) }
    lock_file.puts(Process.pid.to_s)
    lock_file.flush

    # if we blow up so badly that we can't syslog the error, try to send
    # it somewhere useful
    last_ditch_logfile = Settings.last_ditch_logfile || "log/delayed_job.log"
    if last_ditch_logfile[0] != '|'
      last_ditch_logfile = Settings.expand_rails_path(last_ditch_logfile)
    end
    STDIN.reopen("/dev/null")
    STDOUT.reopen(open(last_ditch_logfile, 'a'))
    STDERR.reopen(STDOUT)
    STDOUT.sync = STDERR.sync = true
  end

  # stop the currently running daemon (not this current process, the one in the pid_file)
  def stop(kill: false, pid: self.pid)
    alive = status(pid: pid, print: false)
    if alive == :running || (kill && alive == :draining)
      puts "Stopping pool #{pid}..."
      signal = kill ? 'TERM' : 'QUIT'
      begin
        Process.kill(signal, pid)
      rescue Errno::ESRCH
        # ignore if the pid no longer exists
      end
      wait(kill)
    else
      status
    end
  end

  def wait(kill)
    if kill
      sleep(0.5) while status(pid: pid, print: false)
    else
      sleep(0.5) while status(pid: pid, print: false) == :running
    end
  end

  def pid_file
    File.join(pid_folder, 'delayed_jobs_pool.pid')
  end

  def pid
    if File.file?(pid_file)
      pid = File.read(pid_file).to_i
      pid = nil unless pid > 0
    end
    pid
  end

  def daemonized?
    !!@daemon
  end
end
end
