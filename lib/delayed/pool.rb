require 'erb'
require 'optparse'
require 'yaml'
require 'fileutils'

require 'delayed/daemon/watcher'

module Delayed
class Pool
  mattr_accessor :on_fork
  self.on_fork = ->{ }

  attr_reader :options

  def initialize(args = ARGV)
    @args = args
    @config = { :workers => [] }
    @options = {
      :config_file => expand_rails_path("config/delayed_jobs.yml"),
      :pid_folder => expand_rails_path("tmp/pids"),
      :tail_logs => true, # only in FG mode
    }
  end

  def run
    op = OptionParser.new do |opts|
      opts.banner = "Usage #{$0} <command> <options>"
      opts.separator %{\nWhere <command> is one of:
  start      start the jobs daemon
  stop       stop the jobs daemon
  run        start and run in the foreground
  restart    stop and then start the jobs daemon
  status     show daemon status
}

      opts.separator "\n<options>"
      opts.on("-c", "--config", "Use alternate config file (default #{options[:config_file]})") { |c| options[:config_file] = c }
      opts.on("-p", "--pid", "Use alternate folder for PID files (default #{options[:pid_folder]})") { |p| options[:pid_folder] = p }
      opts.on("--no-tail", "Don't tail the logs (only affects non-daemon mode)") { options[:tail_logs] = false }
      opts.on("--with-prejudice", "When stopping, interrupt jobs in progress, instead of letting them drain") { options[:kill] ||= true }
      opts.on("--with-extreme-prejudice", "When stopping, immediately kill jobs in progress, instead of letting them drain") { options[:kill] = 9 }
      opts.on_tail("-h", "--help", "Show this message") { puts opts; exit }
    end
    op.parse!(@args)

    read_config(options[:config_file])

    command = @args.shift
    case command
    when 'start'
      exit 1 if status(:alive) == :running
      daemonize
      start
    when 'stop'
      stop(options[:kill])
    when 'run'
      start
    when 'status'
      if status
        exit 0
      else
        exit 1
      end
    when 'restart'
      alive = status(false)
      if alive == :running || (options[:kill] && alive == :draining)
        stop(options[:kill])
        if options[:kill]
          sleep(0.5) while status(false)
        else
          sleep(0.5) while status(false) == :running
        end
      end
      daemonize
      start
    when nil
      puts op
    else
      raise("Unknown command: #{command.inspect}")
    end
  end

  protected

  def procname
    "delayed_jobs_pool#{Settings.pool_procname_suffix}"
  end

  def start
    load_rails
    tail_rails_log unless @daemon

    say "Started job master", :info
    $0 = procname
    apply_config

    Watcher.run(UnlockOrphanedJobs.new)
    ## We want the unlocker to finish before continuing, or we might unlock our
    ## own jobs.
    Watcher.join

    Watcher.run(PeriodicAuditor.new)
    spawn_all_workers
    trap(:INT) { Watcher.exiting = true }
    trap(:TERM) { Watcher.exiting = true }
    Watcher.join
    say "Shutting down"
  rescue Exception => e
    say "Job master died with error: #{e.inspect}\n#{e.backtrace.join("\n")}", :fatal
    raise
  end

  def say(msg, level = :debug)
    msg = "[#{Process.pid}]P #{msg}"
    if defined?(Rails.logger) && Rails.logger
      Rails.logger.send(level, msg)
    else
      puts(msg)
    end
  end

  def load_rails
    require(expand_rails_path("config/environment.rb"))
    Dir.chdir(Rails.root)
  end

  def spawn_all_workers
    ActiveRecord::Base.connection_handler.clear_all_connections!

    @config[:workers].each do |worker_config|
      worker_config = worker_config.with_indifferent_access
      next if worker_config[:periodic] # backwards compat
      worker_config = @config.merge(worker_config)
      Watcher.run(WorkerGroup.new(worker_config))
    end
  end

  def tail_rails_log
    return if !@options[:tail_logs]
    path = rails_log_path
    return if !path
    Rails.logger.auto_flushing = true if Rails.logger.respond_to?(:auto_flushing=)
    Thread.new do
      f = File.open(path, 'r')
      f.seek(0, IO::SEEK_END)
      loop do
        content = f.read
        content.present? ? STDOUT.print(content) : sleep(0.5)
      end
    end
  end

  def rails_log_path
    if Rails.logger.respond_to?(:log_path)
      Rails.logger.log_path
    elsif (dev = Rails.logger.instance_variable_get(:@logdev)) && dev.dev.respond_to?(:path)
      dev.dev.path
    else
      nil
    end
  end

  def daemonize
    FileUtils.mkdir_p(pid_folder)
    puts "Daemonizing..."

    exit if fork
    Process.setsid
    exit if fork
    Process.setpgrp

    @daemon = true
    File.open(pid_file, 'wb') { |f| f.write(Process.pid.to_s) }
    # if we blow up so badly that we can't syslog the error, try to send
    # it somewhere useful
    last_ditch_logfile = self.last_ditch_logfile || "log/delayed_job.log"
    if last_ditch_logfile[0] != '|'
      last_ditch_logfile = expand_rails_path(last_ditch_logfile)
    end
    STDIN.reopen("/dev/null")
    STDOUT.reopen(open(last_ditch_logfile, 'a'))
    STDERR.reopen(STDOUT)
    STDOUT.sync = STDERR.sync = true
  end

  def pid_folder
    options[:pid_folder]
  end

  def pid_file
    File.join(pid_folder, 'delayed_jobs_pool.pid')
  end

  def remove_pid_file
    return unless @daemon
    pid = File.read(pid_file) if File.file?(pid_file)
    if pid.to_i == Process.pid
      FileUtils.rm(pid_file)
    end
  end

  def last_ditch_logfile
    @config['last_ditch_logfile']
  end

  def stop(kill = false)
    pid = status(false) && File.read(pid_file).to_i if File.file?(pid_file)
    if pid && pid > 0
      puts "Stopping pool #{pid}..."
      signal = 'INT'
      if kill
        pid = -pid # send to the whole group
        if kill == 9
          signal = 'KILL'
        else
          signal = 'TERM'
        end
      end
      begin
        Process.kill(signal, pid)
      rescue Errno::ESRCH
        # ignore if the pid no longer exists
      end
    else
      status
    end
  end

  def status(print = true)
    pid = File.read(pid_file) if File.file?(pid_file)
    alive = pid && pid.to_i > 0 && (Process.kill(0, pid.to_i) rescue false) && :running
    alive ||= :draining if pid.to_i > 0 && Process.kill(0, -pid.to_i) rescue false
    if alive
      puts "Delayed jobs #{alive}, pool PID: #{pid}" if print
    else
      puts "No delayed jobs pool running" if print && print != :alive
    end
    alive
  end

  def read_config(config_filename)
    config = YAML.load(ERB.new(File.read(config_filename)).result)
    env = defined?(RAILS_ENV) ? RAILS_ENV : ENV['RAILS_ENV'] || 'development'
    @config = config[env] || config['default']
    # Backwards compatibility from when the config was just an array of queues
    @config = { :workers => @config } if @config.is_a?(Array)
    unless @config && @config.is_a?(Hash)
      raise ArgumentError,
        "Invalid config file #{config_filename}"
    end
  end

  def apply_config
    @config = @config.with_indifferent_access
    Settings::SETTINGS.each do |setting|
      Settings.send("#{setting}=", @config[setting.to_s]) if @config.key?(setting.to_s)
    end
  end

  def expand_rails_path(path)
    File.expand_path("../#{path}", ENV['BUNDLE_GEMFILE'])
  end
end

  class UnlockOrphanedJobs < ChildProcess
    def initialize(pid = nil)
      @pid = pid
    end

    def perform
      unlocked_jobs = Delayed::Job.unlock_orphaned_jobs(@pid)
      say "Unlocked #{unlocked_jobs} orphaned jobs" if unlocked_jobs > 0
    end

    def skip?
      Settings.disable_automatic_orphan_unlocking
    end
  end

  class PeriodicAuditor < ChildThread
    def perform
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

    def skip?
      Settings.disable_periodic_jobs
    end

    private

    def schedule_periodic_audit
      Watcher.run(PeriodicAuditQueuer.new)
    end
  end

  # We only run this in a child process to avoid doing real db work in the
  # daemon process.
  class PeriodicAuditQueuer < ChildProcess
    def perform
      Delayed::Periodic.audit_queue
    end

    def exited
      say "ran periodic audit"
    end
  end

  class WorkerGroup < ChildProcess
    def initialize(config)
      @config = config.dup
    end

    def perform
      (@config[:workers] || 1).times { Watcher.run(WorkerProcess.new(@config)) }
      say "Workers spawned"
      Watcher.join do
        Watcher.exiting = true if parent_exited?
      end
    end

    def exited
      say "child exited: #{self.inspect}, restarting", :info
      Watcher.run(self.class.new(@config))
    end
  end

  class WorkerProcess < ChildProcess
    def initialize(config)
      @config = config.dup
      @worker_threads = []
    end

    def perform
      trap(:TERM) { Watcher.exiting = true }
      (@config[:threads_per_process] || 1).times {
        worker = WorkerThread.new(@config)
        @worker_threads << worker
        Watcher.run(worker)
      }
      Watcher.join do
        Watcher.exiting = true if parent_exited?
      end
      @worker_threads.each { |wt| wt.exit! }
      @worker_threads.each { |wt| wt.join }
    end

    def exited
      say "child exited: #{self.inspect}, restarting", :info
      Watcher.run(UnlockOrphanedJobs.new(self.pid))
      Watcher.run(self.class.new(@config))
    end
  end

  class WorkerThread < ChildThread
    attr_reader :worker

    def initialize(config)
      @config = config
      @worker = Delayed::Worker.new(@config)
    end

    def exit!
      @worker.exit = true
    end

    def perform
      @worker.start
    end
  end
end

# There are two types of Tasks: processes, and threads.
# Some tasks want to restart when they exit, some want to just end.
# Processes can be monitored by one global process monitor, which uses Process.wait
# Threads can be monitored by checking status, then calling value to get the return value (or exception).
#
# The tree will look something like:
# (P) process
# (T) thread
# [R] restart if exit
#
# Daemon
# |
# |- (P) Unlock Orphaned Jobs
# |
# |- (T)[R] Audit Scheduler
# |  |
# |  |- (P) Audit Queuer
# |
# |- (P)[R] Worker Group
# |  |
# |  | - (T)[R] Popper
# |  |
# |  |  # Pure process model - should each process just spawn a WorkerThread?
# |  |- (P)[R] WorkerProcess
# |  |- (P)[R] WorkerProcess
# |
# |- (P)[R] Worker Group
# |  |
# |  | - (T)[R] Popper
# |  |
# |  |  # Pure thread model
# |  |- (T)[R] WorkerThread
# |  |- (T)[R] WorkerThread
# |
# |- (P)[R] Worker Group
# |  |
# |  | - (T)[R] Popper
# |  |
# |  |  # Hybrid processes + threads model
# |  |- (P)[R] WorkerProcess
# |  |  |- (T)[R] WorkerThread
# |  |  |- (T)[R] WorkerThread
# |  |  |- (T)[R] WorkerThread
# |  |- (P)[R] WorkerProcess
# |  |  |- (T)[R] WorkerThread
# |  |  |- (T)[R] WorkerThread
# |  |  |- (T)[R] WorkerThread
#
# The daemon does nothing but spawn and monitor tasks.
#
# The worker group has to communicate with the workers, who can come and go.
# Workers will notify the group's popper with their name when they want a job, and the
# popper will batch find+lock jobs in the queue, then distribute them to the
# waiting workers.
# This means serializing the locked jobs to send to WorkerProcess, probably just use Marshal.
#
# The same job daemon can have multiple worker groups, who allow different
# queues/priorities, and who may even be talking to different jobs queues (w/sharding).
# This is why we have a popper per group, rather than one popper for the whole daemon.
