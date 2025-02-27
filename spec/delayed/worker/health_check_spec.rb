# frozen_string_literal: true

require "spec_helper"

RSpec.describe Delayed::Worker::HealthCheck do
  let(:klass) { Class.new(Delayed::Worker::HealthCheck) { self.type_name = :test } }

  before do
    klass # Gotta make sure the class has been defined before we try to use it
  end

  after do
    described_class.subclasses.delete(klass)
  end

  it "must maintain a list of its subclasses" do
    klass
    expect(described_class.subclasses).to include klass
  end

  describe ".build(type:, config: {})" do
    it "must select the concrete class to use by the type_name in the subclass" do
      check = described_class.build(type: "test", worker_name: "foobar")
      expect(check).to be_a(klass)
    end

    it "must raise ArgumentError when the specified type doesn't exist" do
      expect do
        described_class.build(type: "nope", config: { worker_name: "foobar" })
      end.to raise_error ArgumentError
    end

    it "must initiaize the specified class using the supplied config" do
      config = { foo: "bar" }.with_indifferent_access
      check = described_class.build(type: "test", worker_name: "foobar", config:)
      expect(check.config).to eq config
    end
  end

  describe ".reschedule_abandoned_jobs" do
    let(:klass) do
      Class.new(Delayed::Worker::HealthCheck) do
        self.type_name = :fake
        class << self
          attr_accessor :live_workers
        end

        delegate :live_workers, to: :class
      end
    end

    let(:initial_run_at) { 10.minutes.ago }

    before do
      klass.live_workers = %w[alive]
      Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)

      2.times { Delayed::Job.enqueue(SimpleJob.new, run_at: initial_run_at, max_attempts: 4) }
      @alive_job = Delayed::Job.first
      @alive_job.update!({
                           locked_by: "alive",
                           locked_at: initial_run_at
                         })
      @dead_job = Delayed::Job.last
      @dead_job.update!({
                          locked_by: "dead",
                          locked_at: initial_run_at
                        })
      Delayed::Settings.worker_health_check_type = :fake
      Delayed::Settings.worker_health_check_config = {}
    end

    after do
      described_class.subclasses.delete(klass)
      Delayed::Settings.worker_health_check_type = :none
      Delayed::Settings.worker_health_check_config = {}
    end

    it "must leave jobs locked by live workers alone" do
      described_class.reschedule_abandoned_jobs
      @alive_job.reload
      expect(@alive_job.run_at.to_i).to eq initial_run_at.to_i
      expect(@alive_job.locked_at.to_i).to eq initial_run_at.to_i
      expect(@alive_job.locked_by).to eq "alive"
    end

    it "must reschedule jobs locked by dead workers" do
      described_class.reschedule_abandoned_jobs
      @dead_job.reload
      expect(@dead_job.run_at).to be > initial_run_at
      expect(@dead_job.locked_at).to be_nil
      expect(@dead_job.locked_by).to be_nil
    end

    it "ignores jobs that are re-locked after fetching from db" do
      Delayed::Job.where(id: @dead_job).update_all(locked_by: "someone_else")
      # we need to return @dead_job itself, which doesn't match the database
      jobs_scope = double
      allow(jobs_scope).to receive_messages(where: jobs_scope, not: jobs_scope, limit: jobs_scope)
      allow(jobs_scope).to receive(:to_a).and_return([@dead_job], [])
      allow(Delayed::Job).to receive(:running_jobs).and_return(jobs_scope)
      described_class.reschedule_abandoned_jobs
      @dead_job.reload
      expect(@dead_job.locked_by).to eq "someone_else"
    end

    it "ignores jobs that are prefetched" do
      Delayed::Job.where(id: @dead_job).update_all(locked_by: "prefetch:some_node")
      allow(Delayed::Job).to receive(:running_jobs).and_return(Delayed::Job.where(id: @dead_job.id))
      described_class.reschedule_abandoned_jobs
      @dead_job.reload
      expect(@dead_job.locked_by).to eq "prefetch:some_node"
    end

    it "bails immediately if advisory lock already taken" do
      allow(Delayed::Job).to receive(:attempt_advisory_lock).and_return(false)
      described_class.reschedule_abandoned_jobs
      @dead_job.reload
      expect(@dead_job.run_at.to_i).to eq(initial_run_at.to_i)
      expect(@dead_job.locked_at).not_to be_nil
      expect(@dead_job.locked_by).not_to be_nil
    end
  end

  describe "#initialize" do
    it "must raise ArgumentError when the worker name is not supplied" do
      expect { klass.new }.to raise_error ArgumentError
    end
  end
end
