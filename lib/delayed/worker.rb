# frozen_string_literal: true

require "delayed/rails_reloader_plugin"

module Delayed
  class TimeoutError < RuntimeError; end

  class RetriableError < RuntimeError
    # this error is a special case.  You _should_ raise
    # it from inside the rescue block for another error,
    # because it indicates: "something made this job fail
    # but we're pretty sure it's transient and it's safe to try again".
    # the workflow is still the same (retry will happen unless
    # retries are exhausted), but it won't call the :error
    # callback unless it can't retry anymore.  It WILL call the
    # separate ":retry" callback, which is ONLY activated
    # for this kind of error.
  end

  require "tmpdir"
  require "set"

  class Worker
    include Delayed::Logging
    SIGNALS = %i[INT TERM QUIT].freeze

    attr_reader :config, :queue_name, :min_priority, :max_priority, :work_queue

    class << self
      # Callback to fire when a delayed job fails max_attempts times. If this
      # callback is defined, then the value of destroy_failed_jobs is ignored, and
      # the job is destroyed if this block returns true.
      #
      # This allows for destroying "uninteresting" failures, while keeping around
      # interesting failures to be investigated later.
      #
      # The block is called with args(job, last_exception)
      attr_accessor :on_max_failures
    end

    cattr_accessor :plugins
    self.plugins = Set.new

    def self.lifecycle
      @lifecycle ||= Delayed::Lifecycle.new
    end

    def self.current_job
      Thread.current[:running_delayed_job]
    end

    def self.running_job(job)
      Thread.current[:running_delayed_job] = job
      yield
    ensure
      Thread.current[:running_delayed_job] = nil
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
      @health_check_type = Settings.worker_health_check_type
      @health_check_config = Settings.worker_health_check_config
      @config = options
      @job_count = 0

      @signal_queue = []

      plugins << Delayed::RailsReloaderPlugin
      plugins.each(&:inject!)
    end

    def name
      @name ||= "#{Socket.gethostname rescue "X"}:#{id}"
    end

    def process_name=(new_name)
      $0 = "delayed:#{new_name}"
    end

    def exit?
      !!@exit || parent_exited?
    end

    def parent_exited?
      @parent_pid && @parent_pid != Process.ppid
    end

    def wake_up
      @self_pipe[1].write_nonblock(".", exception: false)
      work_queue.wake_up
    end

    def start
      logger.info "Starting worker"
      self.process_name =
        "start:#{Settings.worker_procname_prefix}#{@queue_name}:#{min_priority || 0}:#{max_priority || "max"}"
      @self_pipe = IO.pipe
      work_queue.init

      work_thread = Thread.current
      SIGNALS.each do |sig|
        trap(sig) do
          @signal_queue << sig
          wake_up
        end
      end

      raise "Could not register health_check" unless health_check.start

      signal_processor = Thread.new do
        loop do
          @self_pipe[0].read(1)
          case @signal_queue.pop
          when :INT, :TERM
            @exit = true # get the main thread to bail early if it's waiting for a job
            work_thread.raise(SystemExit) # Force the main thread to bail out of the current job
            cleanup! # we're going to get SIGKILL'd in a moment, so clean up asap
            break
          when :QUIT
            @exit = true
          else
            logger.error "Unknown signal '#{sig}' received"
          end
        end
      end

      self.class.lifecycle.run_callbacks(:execute, self) do
        run until exit?
      end

      logger.info "Stopping worker"
    rescue => e
      Rails.logger.fatal("Child process died: #{e.inspect}") rescue nil
      self.class.lifecycle.run_callbacks(:exceptional_exit, self, e) { nil }
    ensure
      cleanup!

      if signal_processor
        signal_processor.kill
        signal_processor.join
      end

      @self_pipe&.each(&:close)
      @self_pipe = nil
    end

    def cleanup!
      return if cleaned?

      health_check.stop
      work_queue.close
      Delayed::Job.clear_locks!(name)

      @cleaned = true
    end

    def cleaned?
      @cleaned
    end

    def run
      return if exit?

      self.class.lifecycle.run_callbacks(:loop, self) do
        self.process_name =
          "pop:#{Settings.worker_procname_prefix}#{@queue_name}:#{min_priority || 0}:#{max_priority || "max"}"
        job = self.class.lifecycle.run_callbacks(:pop, self) do
          work_queue.get_and_lock_next_available(name, config)
        end

        if job
          configure_for_job(job) do
            @job_count += perform(job)

            if @max_job_count.positive? && @job_count >= @max_job_count
              logger.debug "Max job count of #{@max_job_count} exceeded, dying"
              @exit = true
            end

            if @max_memory_usage.positive?
              memory = sample_memory
              if memory > @max_memory_usage
                logger.debug "Memory usage of #{memory} exceeds max of #{@max_memory_usage}, dying"
                @exit = true
              else
                logger.debug "Memory usage: #{memory}"
              end
            end
          end
        else
          self.process_name =
            "wait:#{Settings.worker_procname_prefix}#{@queue_name}:#{min_priority || 0}:#{max_priority || "max"}"
          sleep(Settings.sleep_delay + (rand * Settings.sleep_delay_stagger)) unless exit?
        end
      end
    end

    def perform(job)
      begin
        count = 1
        raise Delayed::Backend::JobExpired, "job expired at #{job.expires_at}" if job.expired?

        self.class.lifecycle.run_callbacks(:perform, self, job) do
          self.process_name = "run:#{Settings.worker_procname_prefix}#{job.id}:#{job.name}"
          logger.info("Processing #{log_job(job, :long)}")
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
          logger.info("Completed #{log_job(job, :short)} #{format("%.0fms", (runtime * 1000))}")
        end
      rescue ::Delayed::RetriableError => e
        can_retry = job.attempts + 1 < job.inferred_max_attempts
        callback_type = can_retry ? :retry : :error
        self.class.lifecycle.run_callbacks(callback_type, self, job, e) do
          handle_failed_job(job, e)
        end
      rescue SystemExit => e
        # There wasn't really a failure here so no callbacks and whatnot needed,
        # still reschedule the job though.
        job.reschedule(e)
      rescue Exception => e # rubocop:disable Lint/RescueException
        self.class.lifecycle.run_callbacks(:error, self, job, e) do
          handle_failed_job(job, e)
        end
      end
      count
    end

    def perform_batch(parent_job)
      batch = parent_job.payload_object
      return unless batch.mode == :serial

      batch.jobs.each do |job|
        job.source = parent_job.source
        job.create_and_lock!(name)
        configure_for_job(job) do
          perform(job)
        end
      end
      batch.items.size
    end

    def handle_failed_job(job, error)
      job.last_error = "#{error.message}\n#{error.backtrace.join("\n")}"
      logger.error("Failed with #{error.class} [#{error.message}] (#{job.attempts} attempts)")
      job.reschedule(error)
    end

    def id
      Process.pid
    end

    def log_job(job, format = :short)
      case format
      when :long
        "#{job.full_name} #{Settings.job_detailed_log_format.call(job)}"
      else
        "#{job.full_name} #{Settings.job_short_log_format.call(job)}".strip
      end
    end

    # set up the session context information, so that it gets logged with the job log lines
    # also set up a unique tmpdir, which will get removed at the end of the job.
    def configure_for_job(job)
      previous_tmpdir = ENV.fetch("TMPDIR", nil)

      self.class.running_job(job) do
        dir = Dir.mktmpdir("job-#{job.id}-#{name.gsub(/[^\w.]/, ".")}-")
        begin
          ENV["TMPDIR"] = dir
          yield
        ensure
          FileUtils.remove_entry(dir, true)
        end
      end
    ensure
      ENV["TMPDIR"] = previous_tmpdir
    end

    def health_check
      @health_check ||= HealthCheck.build(
        type: @health_check_type,
        worker_name: name,
        config: @health_check_config
      )
    end

    # `sample` reports KB, not B
    if File.directory?("/proc")
      # linux w/ proc fs
      LINUX_PAGE_SIZE = (size = `getconf PAGESIZE`.to_i
                         size.positive? ? size : 4096)
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
