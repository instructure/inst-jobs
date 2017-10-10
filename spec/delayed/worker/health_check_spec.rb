require 'spec_helper'

RSpec.describe Delayed::Worker::HealthCheck do
  let(:klass) { Class.new(Delayed::Worker::HealthCheck) { self.type_name = :test } }

  before do
    klass # Gotta make sure the class has been defined before we try to use it
  end

  after do
    Delayed::Worker::HealthCheck.subclasses.delete(klass)
  end

  it "must maintain a list of its subclasses" do
    klass
    expect(Delayed::Worker::HealthCheck.subclasses).to include klass
  end

  describe '.build(type:, config: {})' do
    it 'must select the concrete class to use by the type_name in the subclass' do
      check = Delayed::Worker::HealthCheck.build(type: 'test', worker_name: 'foobar')
      expect(check).to be_a(klass)
    end

    it "must raise ArgumentError when the specified type doesn't exist" do
      expect {
        Delayed::Worker::HealthCheck.build(type: 'nope', config: {worker_name: 'foobar'})
      }.to raise_error ArgumentError
    end

    it 'must initiaize the specified class using the supplied config' do
      config = {foo: 'bar'}.with_indifferent_access
      check = Delayed::Worker::HealthCheck.build(type: 'test', worker_name: 'foobar', config: config)
      expect(check.config).to eq config
    end
  end

  describe '.reschedule_abandoned_jobs' do
    let(:klass) { Class.new(Delayed::Worker::HealthCheck) {
      self.type_name = :fake
      class << self
        attr_accessor :live_workers
      end

      def live_workers
        self.class.live_workers
      end
    } }

    let(:initial_run_at) { Time.zone.now }

    before do
      klass.live_workers = %w{alive}
      Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)

      2.times { Delayed::Job.enqueue(SimpleJob.new, run_at: initial_run_at, max_attempts: 4) }
      @alive_job = Delayed::Job.first
      @alive_job.update_attributes!({
        locked_by: 'alive',
        locked_at: initial_run_at
      })
      @dead_job = Delayed::Job.last
      @dead_job.update_attributes!({
        locked_by: 'dead',
        locked_at: initial_run_at
      })
      Delayed::Settings.worker_health_check_type = :fake
      Delayed::Settings.worker_health_check_config = {}
      Delayed::Worker::HealthCheck.reschedule_abandoned_jobs
    end

    after do
      Delayed::Worker::HealthCheck.subclasses.delete(klass)
      Delayed::Settings.worker_health_check_type = :none
      Delayed::Settings.worker_health_check_config = {}
    end

    it 'must leave jobs locked by live workers alone' do
      @alive_job.reload
      expect(@alive_job.run_at.to_i).to eq initial_run_at.to_i
      expect(@alive_job.locked_at.to_i).to eq initial_run_at.to_i
      expect(@alive_job.locked_by).to eq 'alive'
    end

    it 'must reschedule jobs locked by dead workers' do
      @dead_job.reload
      expect(@dead_job.run_at).to be > initial_run_at
      expect(@dead_job.locked_at).to be_nil
      expect(@dead_job.locked_by).to be_nil
    end
  end

  describe '#initialize' do
    it 'must raise ArgumentError when the worker name is not supplied' do
      expect { klass.new }.to raise_error ArgumentError
    end
  end
end
