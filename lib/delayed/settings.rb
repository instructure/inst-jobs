module Delayed
  module Settings
    SETTINGS = [ :queue, :max_attempts, :sleep_delay, :sleep_delay_stagger, :fetch_batch_size, :select_random_from_batch, :worker_procname_prefix, :pool_procname_suffix, :default_job_options, :disable_automatic_orphan_unlocking, :disable_periodic_jobs, :periodic_jobs_audit_frequency ]
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
    self.periodic_jobs_audit_frequency = (15*60)

    self.num_strands = ->(strand_name){ nil }
    self.default_job_options = ->{ Hash.new }
  end
end
