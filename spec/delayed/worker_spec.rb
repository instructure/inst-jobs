require_relative "../spec_helper"

module Delayed
  describe Worker do
    let(:worker_config) { {
        queue: "test", min_priority: 1, max_priority: 2, stuff: "stuff",
    }.freeze }
    subject { described_class.new(worker_config.dup) }

    describe "#perform" do
      it "fires off an error callback when a job raises an exception" do
        fired = false
        Worker.lifecycle.before(:error) {|worker, exception| fired = true}
        job = double(:last_error= => nil, attempts: 1, reschedule: nil)
        subject.perform(job)
        expect(fired).to be_truthy
      end
    end

    describe "#run" do
      it "passes extra config options through to the WorkQueue" do
        expect(subject.work_queue).to receive(:get_and_lock_next_available).
          with(subject.name, worker_config).and_return(nil)
        subject.run
      end
    end
  end
end
