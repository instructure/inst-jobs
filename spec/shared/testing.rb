# frozen_string_literal: true

class TestingWorker
  cattr_accessor :runs

  def self.run
    self.runs += 1
  end
end

shared_examples_for "Delayed::Testing" do
  before do
    TestingWorker.runs = 0
  end

  describe ".run_job" do
    it "runs a single queued job" do
      job = TestingWorker.delay(ignore_transaction: true).run
      Delayed::Testing.run_job(job)
      expect(TestingWorker.runs).to eq 1
    end
  end

  describe ".drain" do
    it "runs all queued jobs" do
      3.times { TestingWorker.delay.run }
      YAML.dump(TestingWorker)
      Delayed::Testing.drain
      expect(TestingWorker.runs).to eq 3
    end
  end

  describe "track_created" do
    it "returns the list of jobs created in the block" do
      3.times { TestingWorker.delay.run }
      jobs = Delayed::Testing.track_created { 2.times { TestingWorker.delay.run } }
      expect(jobs.size).to eq 2
      expect(jobs.first.tag).to eq "TestingWorker.run"
    end
  end

  describe "clear_all!" do
    it "deletes all queued jobs" do
      3.times { TestingWorker.delay.run }
      Delayed::Testing.clear_all!
      Delayed::Testing.drain
      expect(TestingWorker.runs).to eq 0
    end
  end
end
