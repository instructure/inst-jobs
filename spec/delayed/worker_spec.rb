require_relative "../spec_helper"

describe Delayed::Worker do
  let(:worker_config) { {
      queue: "test", min_priority: 1, max_priority: 2, stuff: "stuff",
  }.freeze }
  subject { described_class.new(worker_config.dup) }

  after { Delayed::Worker.lifecycle.reset! }

  describe "#perform" do
    it "fires off an error callback when a job raises an exception" do
      fired = false
      Delayed::Worker.lifecycle.before(:error) {|worker, exception| fired = true}
      job = double(:last_error= => nil, attempts: 1, reschedule: nil)
      subject.perform(job)
      expect(fired).to be_truthy
    end

    it "reloads" do
      fakeApplication = double('Rails.application',
          config: double('Rails.application.config',
          cache_classes: false,
          reload_classes_only_on_change: false
        ),
        reloader: double()
      )

      allow(Rails).to receive(:application).and_return(fakeApplication)
      if Rails::VERSION::MAJOR >= 5
        expect(Rails.application.reloader).to receive(:reload!).once
      else
        expect(ActionDispatch::Reloader).to receive(:prepare!).once
        expect(ActionDispatch::Reloader).to receive(:cleanup!).once
      end
      job = double(:last_error= => nil, attempts: 0, reschedule: nil, expired?: false)
      subject.perform(job)
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
