# frozen_string_literal: true

class SimpleJob
  class << self
    attr_accessor :runs
  end

  self.runs = 0

  def perform
    self.class.runs += 1
  end
end

class ErrorJob
  class << self
    attr_accessor :runs, :last_error, :failure_runs, :permanent_failure_runs
  end

  self.runs = 0
  def perform
    raise "did not work"
  end

  self.last_error = nil
  self.failure_runs = 0
  def on_failure(error)
    self.class.last_error = error
    self.class.failure_runs += 1
  end

  self.permanent_failure_runs = 0
  def on_permanent_failure(error)
    self.class.last_error = error
    self.class.permanent_failure_runs += 1
  end
end

class UnlockJob
  attr_accessor :times_to_unlock

  def initialize(times_to_unlock)
    @times_to_unlock = times_to_unlock
  end

  def perform
    raise SystemExit, "raising to trigger on_failure"
  end

  def on_failure(_error)
    times_to_unlock -= 1
    :unlock if times_to_unlock <= 0
  end
end

class LongRunningJob
  def perform
    sleep 250
  end
end

module M
  class ModuleJob
    class << self
      attr_accessor :runs
    end

    cattr_accessor :runs
    self.runs = 0
    def perform
      self.class.runs += 1
    end
  end
end

class DeserializeErrorJob < SimpleJob; end
Psych.add_domain_type("ruby/object", "DeserializeErrorJob") do |_type, _val|
  raise "error deserializing"
end
