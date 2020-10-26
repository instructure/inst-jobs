# frozen_string_literal: true

require 'optparse'

module Delayed
class CLI
  class << self
    attr_accessor :instance
  end

  attr_reader :config

  def initialize(args = ARGV)
    self.class.instance = self

    @args = args
    # config that will be applied on Settings and passed to the created Pool
    @config = {}
    # CLI options that will be kept to this class
    @options = {
      :config_file => Settings.default_worker_config_name,
      :pid_folder => Settings.expand_rails_path("tmp/pids"),
      :tail_logs => true, # only in FG mode
    }
  end

  def run
    parse_cli_options!
    load_and_apply_config!

    command = @args.shift
    case command
    when 'start'
      exit 1 if daemon.status(print: :alive) == :running
      daemon.daemonize!
      start
    when 'stop'
      daemon.stop(kill: @options[:kill])
    when 'run'
      start
    when 'status'
      if daemon.status
        exit 0
      else
        exit 1
      end
    when 'restart'
      daemon.stop(kill: @options[:kill])
      daemon.daemonize!
      start
    when nil
      puts option_parser.to_s
    else
      raise("Unknown command: #{command.inspect}")
    end
  end

  def parse_cli_options!
    option_parser.parse!(@args)
    @options
  end

  protected

  def load_and_apply_config!
    @config = Settings.worker_config(@options[:config_file])
    Settings.apply_worker_config!(@config)
  end

  def option_parser
    @option_parser ||= OptionParser.new do |opts|
      opts.banner = "Usage #{$0} <command> <options>"
      opts.separator %{\nWhere <command> is one of:
  start      start the jobs daemon
  stop       stop the jobs daemon
  run        start and run in the foreground
  restart    stop and then start the jobs daemon
  status     show daemon status
}

      opts.separator "\n<options>"
      opts.on("-c", "--config [CONFIG_PATH]", "Use alternate config file (default #{@options[:config_file]})") { |c| @options[:config_file] = c }
      opts.on("-p", "--pid", "Use alternate folder for PID files (default #{@options[:pid_folder]})") { |p| @options[:pid_folder] = p }
      opts.on("--no-tail", "Don't tail the logs (only affects non-daemon mode)") { @options[:tail_logs] = false }
      opts.on("--with-prejudice", "When stopping, interrupt jobs in progress, instead of letting them drain") { @options[:kill] ||= true }
      opts.on("--with-extreme-prejudice", "When stopping, immediately kill jobs in progress, instead of letting them drain") { @options[:kill] = 9 }
      opts.on_tail("-h", "--help", "Show this message") { puts opts; exit }
    end
  end

  def daemon
    @daemon ||= Delayed::Daemon.new(@options[:pid_folder])
  end

  def start
    load_rails
    tail_rails_log unless daemon.daemonized?
    Delayed::Pool.new(@config).start
  end

  def load_rails
    require(Settings.expand_rails_path("config/environment.rb"))
    Dir.chdir(Rails.root)
  end

  def tail_rails_log
    return if !@options[:tail_logs]
    Delayed::LogTailer.new.run
  end
end
end
