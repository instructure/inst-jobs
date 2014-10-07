require File.expand_path("../spec_helper", __FILE__)

require 'delayed/daemon/queue_proxy'

describe 'Delayed::QueueProxy' do
  before do
    Delayed.select_backend(double())
  end

  let(:proxy) { Delayed::QueueProxy.new }
  after do
    proxy.shutdown!
    Delayed.send(:remove_const, :Job)
  end

  it "should fetch directly when there is no thread" do
    available_job = double()
    worker_name = double()
    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(worker_name, 'queue', 0, 100).and_return(available_job)

    proxy.get_and_lock_next_available(worker_name, 'queue', 0, 100).should == available_job
    proxy.thread.should == nil
    proxy.stats_for_operation(Thread.current, :get).should == 1
  end

  it "should fetch via the thread" do
    available_job = { test: "job" }
    worker_name = double()
    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(worker_name, 'queue', 0, 100).and_return(available_job)

    proxy.run_as_thread
    proxy.get_and_lock_next_available(worker_name, 'queue', 0, 100).should == available_job
    proxy.stats_for_operation(Thread.current, :get).should == 0
    proxy.stats_for_operation(proxy.thread, :get).should == 1
  end
end

