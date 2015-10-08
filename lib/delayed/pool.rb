require 'erb'
require 'optparse'
require 'yaml'
require 'fileutils'

module Delayed
class Pool
  mattr_accessor :on_fork
  self.on_fork = ->{ }

  attr_reader :options, :workers

  def initialize(args = ARGV)
    @args = args
    @workers = {}
    @config = { :workers => [] }
    @options = {
      :config_file => Settings.default_worker_config_name,
      :pid_folder => Settings.expand_rails_path("tmp/pids"),
      :tail_logs => true, # only in FG mode
    }
  end

  def run
    parse_cli_options!

    read_config(options[:config_file])

    command = @args.shift
    case command
    when 'start'
      exit 1 if status(print: :alive) == :running
      daemonize
      start
    when 'stop'
      stop(kill: options[:kill])
    when 'run'
      start
    when 'status'
      if status
        exit 0
      else
        exit 1
      end
    when 'restart'
      pid = self.pid
      alive = status(pid: pid, print: false)
      if alive == :running || (options[:kill] && alive == :draining)
        stop(pid: pid, kill: options[:kill])
        if options[:kill]
          sleep(0.5) while status(pid: pid, print: false)
        else
          sleep(0.5) while status(pid: pid, print: false) == :running
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

  def parse_cli_options!
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
      opts.on("-c", "--config [CONFIG_PATH]", "Use alternate config file (default #{options[:config_file]})") { |c| options[:config_file] = c }
      opts.on("-p", "--pid", "Use alternate folder for PID files (default #{options[:pid_folder]})") { |p| options[:pid_folder] = p }
      opts.on("--no-tail", "Don't tail the logs (only affects non-daemon mode)") { options[:tail_logs] = false }
      opts.on("--with-prejudice", "When stopping, interrupt jobs in progress, instead of letting them drain") { options[:kill] ||= true }
      opts.on("--with-extreme-prejudice", "When stopping, immediately kill jobs in progress, instead of letting them drain") { options[:kill] = 9 }
      opts.on_tail("-h", "--help", "Show this message") { puts opts; exit }
    end
    op.parse!(@args)
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

  def say(msg, level = :debug)
    if defined?(Rails.logger) && Rails.logger
      Rails.logger.send(level, "[#{Process.pid}]P #{msg}")
    else
      puts(msg)
    end
  end

  def load_rails
    require(Settings.expand_rails_path("config/environment.rb"))
    Dir.chdir(Rails.root)
  end

  def unlock_orphaned_jobs(worker = nil, pid = nil)
    # don't bother trying to unlock jobs by process name if the name is overridden
    return if @config.key?(:name)
    return if @config[:disable_automatic_orphan_unlocking]
    return if @config[:workers].any? { |worker_config| worker_config.key?(:name) || worker_config.key?('name') }

    unlocked_jobs = Delayed::Job.unlock_orphaned_jobs(pid)
    say "Unlocked #{unlocked_jobs} orphaned jobs" if unlocked_jobs > 0
    ActiveRecord::Base.connection_handler.clear_all_connections! unless Rails.env.test?
  end

  def spawn_all_workers
    ActiveRecord::Base.connection_handler.clear_all_connections!

    @config[:workers].each do |worker_config|
      worker_config = worker_config.with_indifferent_access
      (worker_config[:workers] || 1).times { spawn_worker(@config.merge(worker_config)) }
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
    return if @config[:disable_periodic_jobs]

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

  def tail_rails_log
    return if !@options[:tail_logs]
    if Rails.logger.respond_to?(:log_path)
      log_path = Rails.logger.log_path
    elsif Rails.logger.instance_variable_get('@logdev').try(:instance_variable_get, '@dev').try(:path)
      log_path = Rails.logger.instance_variable_get('@logdev').instance_variable_get('@dev').path
    else
      return
    end
    Rails.logger.auto_flushing = true if Rails.logger.respond_to?(:auto_flushing=)
    Thread.new do
      f = File.open(log_path, 'r')
      f.seek(0, IO::SEEK_END)
      loop do
        content = f.read
        content.present? ? STDOUT.print(content) : sleep(0.5)
      end
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
    last_ditch_logfile = self.last_ditch_logfile || "log/delayed_job.log"
    if last_ditch_logfile[0] != '|'
      last_ditch_logfile = Settings.expand_rails_path(last_ditch_logfile)
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

  def last_ditch_logfile
    @config['last_ditch_logfile']
  end

  def stop(options = {})
    kill = options[:kill]
    pid = options[:pid] || self.pid
    if pid && status(pid: pid, print: false)
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

  def pid
    if File.file?(pid_file)
      pid = File.read(pid_file).to_i
      pid = nil unless pid > 0
    end
    pid
  end

  def status(options = { print: true })
    print = options[:print]
    pid = options[:pid] || self.pid
    alive = pid && (Process.kill(0, pid) rescue false) && :running
    alive ||= :draining if pid && Process.kill(0, -pid) rescue false
    if alive
      puts "Delayed jobs #{alive}, pool PID: #{pid}" if print
    else
      puts "No delayed jobs pool running" if print && print != :alive
    end
    alive
  end

  def read_config(config_filename)
    @config = Settings.worker_config(config_filename)
  end

  def apply_config
    Settings.apply_worker_config!(@config)
  end

end
end
