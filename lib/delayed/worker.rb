module Delayed

class TimeoutError < RuntimeError; end

require 'tmpdir'
require 'set'

class Worker
  attr_reader :config, :queue_name, :min_priority, :max_priority, :work_queue

  # Callback to fire when a delayed job fails max_attempts times. If this
  # callback is defined, then the value of destroy_failed_jobs is ignored, and
  # the job is destroyed if this block returns true.
  #
  # This allows for destroying "uninteresting" failures, while keeping around
  # interesting failures to be investigated later.
  #
  # The block is called with args(job, last_exception)
  def self.on_max_failures=(block)
    @@on_max_failures = block
  end
  cattr_reader :on_max_failures

  cattr_accessor :plugins
  self.plugins = Set.new

  def self.lifecycle
    @lifecycle ||= Delayed::Lifecycle.new
  end

  def initialize(options = {})
    @exit = false
    @parent_pid = options[:parent_pid]
    @queue_name = options[:queue] ||= Settings.queue
    @min_priority = options[:min_priority]
    @max_priority = options[:max_priority]
    @max_job_count = options[:worker_max_job_count].to_i
    @max_memory_usage = options[:worker_max_memory_usage].to_i
    @work_queue = options.delete(:work_queue) || WorkQueue::InProcess.new
    @config = options
    @job_count = 0

    app = Rails.application
    if app && !app.config.cache_classes
      Delayed::Worker.lifecycle.around(:perform) do |&block|
        reload = app.config.reload_classes_only_on_change != true || app.reloaders.map(&:updated?).any?
        ActionDispatch::Reloader.prepare! if reload
        begin
          block.call
        ensure
          ActionDispatch::Reloader.cleanup! if reload
        end
      end
    end

    plugins.each { |plugin| plugin.inject! }
  end

  def name
    @name ||= "#{Socket.gethostname rescue "X"}:#{self.id}"
  end

  def set_process_name(new_name)
    $0 = "delayed:#{new_name}"
  end

  def exit?
    @exit || parent_exited?
  end

  def parent_exited?
    @parent_pid && @parent_pid != Process.ppid
  end

  def start
    say "Starting worker", :info

    trap('INT') { say 'Exiting'; @exit = true }

    self.class.lifecycle.run_callbacks(:execute, self) do
      loop do
        run
        break if exit?
      end
    end

    say "Stopping worker", :info
  rescue => e
    Rails.logger.fatal("Child process died: #{e.inspect}") rescue nil
    self.class.lifecycle.run_callbacks(:exceptional_exit, self, e) { }
  ensure
    Delayed::Job.clear_locks!(name)
  end

  def run
    self.class.lifecycle.run_callbacks(:loop, self) do
      job = self.class.lifecycle.run_callbacks(:pop, self) do
        work_queue.get_and_lock_next_available(name, config)
      end

      if job
        configure_for_job(job) do
          @job_count += perform(job)

          if @max_job_count > 0 && @job_count >= @max_job_count
            say "Max job count of #{@max_job_count} exceeded, dying"
            @exit = true
          end

          if @max_memory_usage > 0
            memory = sample_memory
            if memory > @max_memory_usage
              say "Memory usage of #{memory} exceeds max of #{@max_memory_usage}, dying"
              @exit = true
            else
              say "Memory usage: #{memory}"
            end
          end
        end
      else
        set_process_name("wait:#{Settings.worker_procname_prefix}#{@queue_name}:#{min_priority || 0}:#{max_priority || 'max'}")
        sleep(Settings.sleep_delay + (rand * Settings.sleep_delay_stagger))
      end
    end
  end

  def perform(job)
    count = 1
    raise Delayed::Backend::JobExpired, "job expired at #{job.expires_at}" if job.expired?
    self.class.lifecycle.run_callbacks(:perform, self, job) do
      set_process_name("run:#{Settings.worker_procname_prefix}#{job.id}:#{job.name}")
      say("Processing #{log_job(job, :long)}", :info)
      runtime = Benchmark.realtime do
        if job.batch?
          # each job in the batch will have perform called on it, so we don't
          # need a timeout around this
          count = perform_batch(job)
        else
          job.invoke_job
        end
        job.destroy
      end
      say("Completed #{log_job(job)} #{"%.0fms" % (runtime * 1000)}", :info)
    end
    count
  rescue Exception => e
    self.class.lifecycle.run_callbacks(:error, self, job, e) do
      handle_failed_job(job, e)
    end
    count
  end

  def perform_batch(parent_job)
    batch = parent_job.payload_object
    if batch.mode == :serial
      batch.jobs.each do |job|
        job.source = parent_job.source
        job.create_and_lock!(name)
        configure_for_job(job) do
          perform(job)
        end
      end
      batch.items.size
    end
  end

  def handle_failed_job(job, error)
    job.last_error = "#{error.message}\n#{error.backtrace.join("\n")}"
    say("Failed with #{error.class} [#{error.message}] (#{job.attempts} attempts)", :error)
    job.reschedule(error)
  end

  def id
    Process.pid
  end

  def say(msg, level = :debug)
    Rails.logger.send(level, msg)
  end

  def log_job(job, format = :short)
    case format
    when :long
      "#{job.full_name} #{ job.to_json(:include_root => false, :only => %w(tag strand priority attempts created_at max_attempts source)) }"
    else
      job.full_name
    end
  end

  # set up the session context information, so that it gets logged with the job log lines
  # also set up a unique tmpdir, which will get removed at the end of the job.
  def configure_for_job(job)
    previous_tmpdir = ENV['TMPDIR']
    Thread.current[:running_delayed_job] = job

    dir = Dir.mktmpdir("job-#{job.id}-#{self.name.gsub(/[^\w\.]/, '.')}-")
    begin
      ENV['TMPDIR'] = dir
      yield
    ensure
      FileUtils.remove_entry(dir, true)
    end
  ensure
    ENV['TMPDIR'] = previous_tmpdir
    Thread.current[:running_delayed_job] = nil
  end

  def self.current_job
    Thread.current[:running_delayed_job]
  end

  # `sample` reports KB, not B
  if File.directory?("/proc")
    # linux w/ proc fs
    LINUX_PAGE_SIZE = (size = `getconf PAGESIZE`.to_i; size > 0 ? size : 4096)
    def sample_memory
      s = File.read("/proc/#{Process.pid}/statm").to_i rescue 0
      s * LINUX_PAGE_SIZE / 1024
    end
  else
    # generic unix solution
    def sample_memory
      if Rails.env.test?
        0
      else
        # hmm this is actually resident set size, doesn't include swapped-to-disk
        # memory.
        `ps -o rss= -p #{Process.pid}`.to_i
      end
    end
  end

end
end
