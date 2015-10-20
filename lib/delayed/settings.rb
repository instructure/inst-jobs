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
      config.with_indifferent_access
    end

    def self.apply_worker_config!(config)
      SETTINGS.each do |setting|
        self.send("#{setting}=", config[setting.to_s]) if config.key?(setting.to_s)
      end
    end

    def self.default_worker_config_name
      expand_rails_path("config/delayed_jobs.yml")
    end

    # Expands rails-relative paths, without depending on rails being loaded.
    def self.expand_rails_path(path)
      root = if defined?(Rails) && Rails.root
        (Rails.root+"Gemfile").to_s
      else
        ENV.fetch('BUNDLE_GEMFILE')
      end
      File.expand_path("../#{path}", root)
    end
  end
end
