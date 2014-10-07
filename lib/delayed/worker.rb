module Delayed

class TimeoutError < RuntimeError; end

require 'tmpdir'

require 'delayed/daemon/task'

class Worker < Task
  attr_reader :config, :queue, :min_priority, :max_priority
  attr_reader :worker_name

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

  def self.lifecycle
    @lifecycle ||= Delayed::Lifecycle.new
  end

  def initialize(options = {})
    @exit = false
    @config = options.dup
    @queue = options[:queue] || Settings.queue
    @min_priority = options[:min_priority]
    @max_priority = options[:max_priority]
    @max_job_count = options[:worker_max_job_count].to_i
    @max_memory_usage = options[:worker_max_memory_usage].to_i
    @worker_name = options[:worker_name]
    @job_count = 0
    self.description = process_name("init")

    raise(ArgumentError, "must define worker_name") unless worker_name
  end

  def process_name(current_task, queue_config = true)
    max_priority = @config[:max_priority] == Delayed::MAX_PRIORITY ? 'max' : @config[:max_priority]
    if queue_config
      queue_data = ":#{@config[:queue]}:#{@config[:min_priority]}:#{max_priority}:#{@config[:threads_per_process]}"
    end
    "delayed#{Settings.pool_procname_suffix}:worker_task:#{Settings.worker_procname_prefix}#{current_task}#{queue_data}"
  end

  def execute
    start
  end

  def start
    run_loop do
      run
    end
  ensure
    Delayed::Job.clear_locks!(worker_name)
  end

  def run
    job =
        self.class.lifecycle.run_callbacks(:pop, self) do
          Delayed::Job.get_and_lock_next_available(
            worker_name,
            queue,
            min_priority,
            max_priority)
        end

    if job
      configure_for_job(job) do
        @job_count += perform(job)

        if @max_job_count > 0 && @job_count >= @max_job_count
          say "Max job count of #{@max_job_count} exceeded, dying"
          @exit = true
        end

        if process? && @max_memory_usage > 0
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
      self.description = process_name('wait')
      sleep(Settings.sleep_delay + (rand * Settings.sleep_delay_stagger))
    end
  end

  def perform(job)
    count = 1
    self.class.lifecycle.run_callbacks(:perform, self, job) do
      self.description = process_name("#{job.id}:#{job.name}", false)
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
    handle_failed_job(job, e)
    count
  end

  def perform_batch(parent_job)
    batch = parent_job.payload_object
    if batch.mode == :serial
      batch.jobs.each do |job|
        job.source = parent_job.source
        job.create_and_lock!(worker_name)
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

  def say(msg, level = :debug)
    Rails.logger.send(level, msg)
  end

  def log_job(job, format = :short)
    case format
    when :long
      "#{job.full_name} #{ job.to_json(:include_root => false, :only => %w(id run_at tag strand priority attempts created_at max_attempts source)) }"
    else
      job.full_name
    end
  end

  # Set up the session context information, so that it gets logged with the job log lines.
  # Also set up a unique tmpdir, which will get removed at the end of the job (only if single threaded).
  def configure_for_job(job)
    Thread.current[:running_delayed_job] = job
    if process?
      with_tmpdir(job) { yield }
    else
      yield
    end
  ensure
    Thread.current[:running_delayed_job] = nil
  end

  def self.current_job
    Thread.current[:running_delayed_job]
  end

  def with_tmpdir(job)
    previous_tmpdir = ENV['TMPDIR']
    Dir.mktmpdir("job-#{job.id}-#{self.worker_name.gsub(/[^\w\.]/, '.')}-") do |dir|
      ENV['TMPDIR'] = dir
      yield
    end
  ensure
    ENV['TMPDIR'] = previous_tmpdir
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
