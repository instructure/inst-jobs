# frozen_string_literal: true

require "spec_helper"

RSpec.describe Delayed::Periodic do
  around do |block|
    # make sure we can use ".cron" and
    # such safely without leaking global state
    prev_sched = described_class.scheduled
    prev_ovr = described_class.overrides
    described_class.scheduled = {}
    described_class.overrides = {}
    block.call
  ensure
    described_class.scheduled = prev_sched
    described_class.overrides = prev_ovr
    Delayed::Job.delete_all
  end

  describe ".cron" do
    let(:job_name) { "just a test" }

    it "provides a tag by default for periodic jobs" do
      described_class.cron job_name, "*/10 * * * *" do
        # no-op
      end
      instance = described_class.scheduled[job_name]
      expect(instance).not_to be_nil
      expect(instance.enqueue_args[:singleton]).to eq("periodic: just a test")
    end
  end
end
