require 'spec_helper'

RSpec.describe Delayed::WorkQueue::ParentProcess::Client do
  let(:subject) { described_class.new(addrinfo) }
  let(:addrinfo) { double('Addrinfo') }
  let(:connection) { double('Socket') }
  let(:job) { Delayed::Job.new(locked_by: "worker_name") }
  let(:worker_config) { { queue: "queue_name", min_priority: 1, max_priority: 2 } }
  let(:args) { ["worker_name", worker_config] }
  let(:job_args) { [["worker_name"], "queue_name", 1, 2] }

  before :all do
    FileUtils.mkdir_p(Delayed::Settings.expand_rails_path('tmp'))
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
  end

  after :all do
    Delayed.send(:remove_const, :Job)
  end

  it 'marshals the given arguments to the server and returns the response' do
    expect(addrinfo).to receive(:connect).once.and_return(connection)
    expect(Marshal).to receive(:dump).with(args, connection).ordered
    expect(Marshal).to receive(:load).with(connection).and_return(job).ordered
    response = subject.get_and_lock_next_available(*args)
    expect(response).to eq(job)
  end

  it 'returns nil and then reconnects on socket error' do
    expect(addrinfo).to receive(:connect).once.and_return(connection)
    expect(Marshal).to receive(:dump).and_raise(SystemCallError.new("failure"))
    response = subject.get_and_lock_next_available(*args)
    expect(response).to be_nil

    expect(addrinfo).to receive(:connect).once.and_return(connection)
    expect(Marshal).to receive(:dump).with(args, connection)
    expect(Marshal).to receive(:load).with(connection).and_return(job)
    response = subject.get_and_lock_next_available(*args)
    expect(response).to eq(job)
  end

  it 'errors if the response is not a locked job' do
    expect(addrinfo).to receive(:connect).once.and_return(connection)
    expect(Marshal).to receive(:dump).with(args, connection)
    expect(Marshal).to receive(:load).with(connection).and_return(:not_a_job)
    expect { subject.get_and_lock_next_available(*args) }.to raise_error(Delayed::WorkQueue::ParentProcess::ProtocolError)
  end

  it 'errors if the response is a job not locked by this worker' do
    expect(addrinfo).to receive(:connect).once.and_return(connection)
    expect(Marshal).to receive(:dump).with(args, connection)
    job.locked_by = "somebody_else"
    expect(Marshal).to receive(:load).with(connection).and_return(job)
    expect { subject.get_and_lock_next_available(*args) }.to raise_error(Delayed::WorkQueue::ParentProcess::ProtocolError)
  end
end
