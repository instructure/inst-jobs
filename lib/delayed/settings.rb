# frozen_string_literal: true

require "yaml"
require "erb"
require "active_support/core_ext/hash/indifferent_access"

module Delayed
  module Settings
    SETTINGS = [
      :default_job_options,
      :disable_abandoned_job_cleanup,
      :disable_periodic_jobs,
      :disable_automatic_orphan_unlocking,
      :fetch_batch_size,
      # this is a transitional setting, so that you don't have a time where a
      # singleton switches to using the singleton column, but there are old
      # jobs that only used strand
      :infer_strand_from_singleton,
      :kill_workers_on_exit,
      :last_ditch_logfile,
      :max_attempts,
      :pool_procname_suffix,
      :queue,
      :select_random_from_batch,
      :silence_periodic_log,
      :sleep_delay,
      :sleep_delay_stagger,
      :slow_exit_timeout,
      :worker_health_check_type,
      :worker_health_check_config,
      :worker_procname_prefix
    ].freeze

    SETTINGS_WITH_ARGS = %i[
      job_detailed_log_format
      num_strands
    ].freeze

    PARENT_PROCESS_DEFAULTS = {
      server_socket_timeout: 10.0,
      prefetched_jobs_timeout: 30.0,

      client_connect_timeout: 2.0,

      # We'll accept a partial, relative path and assume we want it inside
      # Rails.root with inst-jobs.sock appended if provided a directory.
      server_address: "tmp"
    }.with_indifferent_access.freeze

    class << self
      attr_accessor(*SETTINGS_WITH_ARGS)
      attr_reader :parent_process

      SETTINGS.each do |setting|
        attr_writer setting

        define_method(setting) do
          val = instance_variable_get(:"@#{setting}")
          val.respond_to?(:call) ? val.call : val
        end
      end

      def queue=(queue_name)
        raise ArgumentError, "queue_name must not be blank" if queue_name.blank?

        @queue = queue_name
      end

      def worker_config(config_filename = nil)
        config_filename ||= default_worker_config_name
        config = YAML.load(ERB.new(File.read(config_filename)).result)
        env = Rails.env || "development"
        config = config[env] || config["default"]
        # Backwards compatibility from when the config was just an array of queues
        config = { workers: config } if config.is_a?(Array)
        unless config.is_a?(Hash)
          raise ArgumentError,
                "Invalid config file #{config_filename}"
        end
        config = config.with_indifferent_access
        config[:workers].map! do |worker_config|
          config.except(:workers).merge(worker_config.with_indifferent_access)
        end
        config
      end

      def apply_worker_config!(config)
        SETTINGS.each do |setting|
          send("#{setting}=", config[setting.to_s]) if config.key?(setting.to_s)
        end
        if config.key?("parent_process_client_timeout")
          parent_process.client_timeout = config["parent_process_client_timeout"]
        end
        self.parent_process = config["parent_process"] if config.key?("parent_process")
      end

      def default_worker_config_name
        expand_rails_path("config/delayed_jobs.yml")
      end

      # Expands rails-relative paths, without depending on rails being loaded.
      def expand_rails_path(path)
        root = if defined?(Rails) && Rails.root
                 Rails.root.join("Gemfile")
               else
                 ENV.fetch("BUNDLE_GEMFILE", "#{Dir.pwd}/Gemfile")
               end
        File.expand_path("../#{path}", root)
      end

      def parent_process_client_timeout=(val)
        parent_process["server_socket_timeout"] = Integer(val)
      end

      def parent_process=(new_config)
        raise "Parent process configurations must be a hash!" unless new_config.is_a?(Hash)

        @parent_process = PARENT_PROCESS_DEFAULTS.merge(new_config)
      end

      def worker_health_check_config=(new_config)
        @worker_health_check_config = (new_config || {}).with_indifferent_access
      end
    end

    self.parent_process = PARENT_PROCESS_DEFAULTS.dup
    self.queue = "queue"
    self.max_attempts = 1
    self.sleep_delay = 2.0
    self.sleep_delay_stagger = 2.0
    self.fetch_batch_size = 5
    self.select_random_from_batch = false
    self.silence_periodic_log = false

    self.num_strands = ->(_strand_name) {}
    self.default_job_options = -> { {} }
    self.job_detailed_log_format = lambda { |job|
      job.to_json(include_root: false, only: %w[tag strand priority attempts created_at max_attempts source])
    }

    # Send workers KILL after QUIT if they haven't exited within the
    # slow_exit_timeout
    self.kill_workers_on_exit = true
    self.slow_exit_timeout = 20

    self.worker_health_check_type = :none
    self.worker_health_check_config = {}
  end
end
