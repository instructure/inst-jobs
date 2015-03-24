require_relative "../spec_helper"

module Delayed
  describe Worker do
    describe "#perform" do
      it "fires off an error callback when a job raises an exception" do
        fired = false
        Worker.lifecycle.before(:error) {|worker, exception| fired = true}
        worker = Worker.new
        job = double(:last_error= => nil, attempts: 1, reschedule: nil)
        worker.perform(job)
        expect(fired).to be_truthy
      end
    end
  end
end
