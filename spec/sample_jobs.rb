class SimpleJob
  cattr_accessor :runs; self.runs = 0
  def perform; @@runs += 1; end
end

class ErrorJob
  cattr_accessor :runs; self.runs = 0
  def perform; raise 'did not work'; end

  cattr_accessor :last_error; self.last_error = nil

  cattr_accessor :failure_runs; self.failure_runs = 0
  def on_failure(error); @@last_error = error; @@failure_runs += 1; end

  cattr_accessor :permanent_failure_runs; self.permanent_failure_runs = 0
  def on_permanent_failure(error); @@last_error = error; @@permanent_failure_runs += 1; end
end

class UnlockJob
  attr_accessor :times_to_unlock
  def initialize(times_to_unlock)
    @times_to_unlock = times_to_unlock
  end

  def perform; raise SystemExit, 'raising to trigger on_failure'; end

  def on_failure(error)
    times_to_unlock -= 1
    :unlock if times_to_unlock <= 0
  end
end

class LongRunningJob
  def perform; sleep 250; end
end

module M
  class ModuleJob
    cattr_accessor :runs; self.runs = 0
    def perform; @@runs += 1; end
  end
end

class DeserializeErrorJob < SimpleJob; end
Psych.add_domain_type("ruby/object", "DeserializeErrorJob") do |_type, val|
  raise "error deserializing"
end
