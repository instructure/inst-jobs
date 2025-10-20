# frozen_string_literal: true

module Delayed
  MIN_PRIORITY = 0
  HIGH_PRIORITY = 0
  NORMAL_PRIORITY = 10
  LOW_PRIORITY = 20
  LOWER_PRIORITY = 50
  MAX_PRIORITY = 1_000_000

  def self.select_backend(backend)
    remove_const(:Job) if defined?(::Delayed::Job)
    const_set(:Job, backend)
  end
end

require "rails"
require "active_support/core_ext/module/attribute_accessors"
require "active_record"
require "after_transaction_commit"
require "debug_inspector"

require "delayed/core_ext/kernel"

require "delayed/settings"
require "delayed/yaml_extensions"

require "delayed/backend/base"
require "delayed/backend/active_record"
require "delayed/batch"
require "delayed/cli"
require "delayed/daemon"
require "delayed/job_tracking"
require "delayed/lifecycle"
require "delayed/log_tailer"
require "delayed/logging"
require "delayed/message_sending"
require "delayed/performable_method"
require "delayed/periodic"
require "delayed/plugin"
require "delayed/pool"
require "delayed/worker"
require "delayed/util"

require "delayed/worker/health_check"
require "delayed/worker/consul_health_check"
require "delayed/worker/null_health_check"

require "delayed/work_queue/in_process"
require "delayed/work_queue/parent_process"

require "delayed/engine"

Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)

Object.include Delayed::MessageSending
Module.include Delayed::MessageSending::ClassMethods
