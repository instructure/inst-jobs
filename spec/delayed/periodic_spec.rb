# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Delayed::Periodic do
  around(:each) do |block|
    # make sure we can use ".cron" and
    # such safely without leaking global state
    prev_sched = Delayed::Periodic.scheduled
    prev_ovr = Delayed::Periodic.overrides
    Delayed::Periodic.scheduled = {}
    Delayed::Periodic.overrides = {}
    block.call
  ensure
    Delayed::Periodic.scheduled = prev_sched
    Delayed::Periodic.overrides = prev_ovr
  end

  describe ".cron" do
    let(:job_name){ 'just a test'}
    it "provides a tag by default for periodic jobs" do
      Delayed::Periodic.cron job_name, '*/10 * * * *' do
        # no-op
      end
      instance = Delayed::Periodic.scheduled[job_name]
      expect(instance).to_not be_nil
      expect(instance.enqueue_args[:singleton]).to eq("periodic: just a test")
    end

    it "uses no singleton if told to skip" do
      Delayed::Periodic.cron job_name, '*/10 * * * *', {singleton: false} do
        # no-op
      end
      instance = Delayed::Periodic.scheduled[job_name]
      expect(instance).to_not be_nil
      expect(instance.enqueue_args[:singleton]).to be_nil
    end
  end
end
