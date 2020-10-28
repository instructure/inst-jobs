# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Delayed::WorkQueue::InProcess do
  before :all do
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
  end

  after :each do
    Delayed::Worker.lifecycle.reset!
  end

  let(:subject) { described_class.new }
  let(:worker_config) { {queue: "test", min_priority: 1, max_priority: 2} }
  let(:args) { ["worker_name", worker_config] }

  it 'triggers the lifecycle event around the pop' do
    called = false
    Delayed::Worker.lifecycle.around(:work_queue_pop) do |queue, &cb|
      expect(queue).to eq(subject)
      expect(Delayed::Job).to receive(:get_and_lock_next_available).
        with("worker_name", "test", 1, 2).
        and_return(:job)
      called = true
      cb.call(queue)
    end
    job = subject.get_and_lock_next_available(*args)
    expect(job).to eq(:job)
    expect(called).to eq(true)
  end
end
