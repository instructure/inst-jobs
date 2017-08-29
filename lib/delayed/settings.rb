require 'yaml'
require 'erb'
require 'active_support/core_ext/hash/indifferent_access'

module Delayed
  module Settings
    SETTINGS = [
      :queue,
      :max_attempts,
      :sleep_delay,
      :sleep_delay_stagger,
      :fetch_batch_size,
      :select_random_from_batch,
      :worker_procname_prefix,
      :pool_procname_suffix,
      :default_job_options,
      :silence_periodic_log,
      :disable_periodic_jobs,
      :disable_automatic_orphan_unlocking,
      :last_ditch_logfile,
    ]
    SETTINGS_WITH_ARGS = [ :num_strands ]

    SETTINGS.each do |setting|
      mattr_writer(setting)
      self.send("#{setting}=", nil)
      define_singleton_method(setting) do
        val = class_variable_get(:"@@#{setting}")
        val.respond_to?(:call) ? val.call() : val
      end
    end

    mattr_accessor(*SETTINGS_WITH_ARGS)

    PARENT_PROCESS_DEFAULTS = {
      server_socket_timeout: 10.0,
      prefetched_jobs_timeout: 30.0,

      client_connect_timeout: 2.0,

      # We'll accept a partial, relative path and assume we want it inside
      # Rails.root with inst-jobs.sock appended if provided a directory.
      server_address: 'tmp',
    }.with_indifferent_access.freeze

    mattr_reader(:parent_process)
    @@parent_process = PARENT_PROCESS_DEFAULTS.dup

    def self.queue=(queue_name)
      raise(ArgumentError, "queue_name must not be blank") if queue_name.blank?
      @@queue = queue_name
    end

    self.queue = "queue"
    self.max_attempts = 1
    self.sleep_delay = 2.0
    self.sleep_delay_stagger = 2.0
    self.fetch_batch_size = 5
    self.select_random_from_batch = false
    self.silence_periodic_log = false

    self.num_strands = ->(strand_name){ nil }
    self.default_job_options = ->{ Hash.new }

    def self.worker_config(config_filename = nil)
      config_filename ||= default_worker_config_name
      config = YAML.load(ERB.new(File.read(config_filename)).result)
      env = defined?(RAILS_ENV) ? RAILS_ENV : ENV['RAILS_ENV'] || 'development'
      config = config[env] || config['default']
      # Backwards compatibility from when the config was just an array of queues
      config = { :workers => config } if config.is_a?(Array)
      unless config && config.is_a?(Hash)
        raise ArgumentError,
          "Invalid config file #{config_filename}"
      end
      config = config.with_indifferent_access
      config[:workers].map! do |worker_config|
        config.except(:workers).merge(worker_config.with_indifferent_access)
      end
      config
    end

    def self.apply_worker_config!(config)
      SETTINGS.each do |setting|
        self.send("#{setting}=", config[setting.to_s]) if config.key?(setting.to_s)
      end
      parent_process.client_timeout = config['parent_process_client_timeout'] if config.key?('parent_process_client_timeout')
      self.parent_process = config['parent_process'] if config.key?('parent_process')
    end

    def self.default_worker_config_name
      expand_rails_path("config/delayed_jobs.yml")
    end

    # Expands rails-relative paths, without depending on rails being loaded.
    def self.expand_rails_path(path)
      root = if defined?(Rails) && Rails.root
        (Rails.root+"Gemfile").to_s
      else
        ENV.fetch('BUNDLE_GEMFILE', Dir.pwd+"/Gemfile")
      end
      File.expand_path("../#{path}", root)
    end

    def self.parent_process_client_timeout=(val)
      parent_process['server_socket_timeout'] = Integer(val)
    end

    def self.parent_process=(new_config)
      raise 'Parent process configurations must be a hash!' unless Hash === new_config
      @@parent_process = PARENT_PROCESS_DEFAULTS.merge(new_config)
    end
  end
end
