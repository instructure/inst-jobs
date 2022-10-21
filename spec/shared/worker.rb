# frozen_string_literal: true

class TestPlugin < ::Delayed::Plugin
  cattr_accessor :runs
  self.runs = 0
  callbacks do |lifecycle|
    lifecycle.around(:invoke_job) do |job, *args, &block|
      TestPlugin.runs += 1
      block.call(job, *args)
    end
  end
end

shared_examples_for "Delayed::Worker" do
  def job_create(opts = {})
    Delayed::Job.create({ payload_object: SimpleJob.new, queue: Delayed::Settings.queue }.merge(opts))
  end

  def worker_create(opts = {})
    Delayed::Worker.new(opts.merge(max_priority: nil, min_priority: nil, quiet: true))
  end

  before do
    @worker = worker_create
    SimpleJob.runs = 0
    Delayed::Worker.on_max_failures = nil
    Delayed::Settings.sleep_delay = -> { 0.01 }
  end

  after do
    Delayed::Settings.sleep_delay = 2.0
  end

  describe "running a job" do
    it "does not fail when running a job with a % in the name" do
      expect do
        @job = "Some % Name here".delay(ignore_transaction: true).start_with?("Some % Name")
        @worker.perform(@job)
      end.not_to raise_error
    end
  end

  describe "running a batch" do
    context "serially" do
      before do
        @runs = 0
        Delayed::Worker.lifecycle.after(:perform) { @runs += 1 }
      end

      after do
        Delayed::Worker.lifecycle.reset!
      end

      it "runs each job in order" do
        bar = +"bar"
        expect(bar).to receive(:scan).with("b").ordered
        expect(bar).to receive(:scan).with("a").ordered
        expect(bar).to receive(:scan).with("r").ordered
        batch = Delayed::Batch::PerformableBatch.new(:serial, [
                                                       { payload_object: Delayed::PerformableMethod.new(bar, :scan,
                                                                                                        args: ["b"]) },
                                                       { payload_object: Delayed::PerformableMethod.new(bar, :scan,
                                                                                                        args: ["a"]) },
                                                       { payload_object: Delayed::PerformableMethod.new(bar, :scan,
                                                                                                        args: ["r"]) }
                                                     ])

        batch_job = Delayed::Job.create payload_object: batch
        expect(@worker.perform(batch_job)).to eq(3)
        expect(@runs).to be 4 # batch, plus all jobs
      end

      it "succeeds regardless of the success/failure of its component jobs" do
        change_setting(Delayed::Settings, :max_attempts, 2) do
          batch = Delayed::Batch::PerformableBatch.new(:serial, [
                                                         { payload_object: Delayed::PerformableMethod.new("foo",
                                                                                                          :reverse) },
                                                         { payload_object: Delayed::PerformableMethod.new(1, :/,
                                                                                                          args: [0]) },
                                                         { payload_object: Delayed::PerformableMethod.new("bar", :scan,
                                                                                                          args: ["r"]) }
                                                       ])
          batch_job = Delayed::Job.create payload_object: batch

          expect(@worker.perform(batch_job)).to eq(3)
          expect(@runs).to be 3 # batch, plus two successful jobs

          to_retry = Delayed::Job.list_jobs(:future, 100)
          expect(to_retry.size).to be 1
          expect(to_retry[0].payload_object.method).to be :/
          expect(to_retry[0].last_error).to match(/divided by 0/)
          expect(to_retry[0].attempts).to eq(1)
        end
      end

      it "retries a failed individual job" do
        batch = Delayed::Batch::PerformableBatch.new(:serial, [
                                                       { payload_object: Delayed::PerformableMethod.new(1,
                                                                                                        :/,
                                                                                                        args: [0]) }
                                                     ])
        batch_job = Delayed::Job.create payload_object: batch

        expect_any_instance_of(Delayed::Job).to receive(:reschedule).once
        expect(@worker.perform(batch_job)).to eq(1)
        expect(@runs).to be 1 # just the batch
      end
    end
  end

  context "worker prioritization" do
    before do
      @worker = Delayed::Worker.new(max_priority: 5, min_priority: 2, quiet: true)
    end

    it "onlies run jobs that are >= min_priority" do
      expect(SimpleJob.runs).to eq(0)

      job_create(priority: 1)
      job_create(priority: 3)
      @worker.run

      expect(SimpleJob.runs).to eq(1)
    end

    it "onlies run jobs that are <= max_priority" do
      expect(SimpleJob.runs).to eq(0)

      job_create(priority: 10)
      job_create(priority: 4)

      @worker.run

      expect(SimpleJob.runs).to eq(1)
    end
  end

  context "while running with locked jobs" do
    it "does not run jobs locked by another worker" do
      job_create(locked_by: "other_worker", locked_at: (Delayed::Job.db_time_now - 1.minute))
      expect { @worker.run }.not_to(change(SimpleJob, :runs))
    end

    it "runs open jobs" do
      job_create
      expect { @worker.run }.to change(SimpleJob, :runs).from(0).to(1)
    end
  end

  describe "failed jobs" do
    before do
      # reset defaults
      Delayed::Settings.max_attempts = 25
      @job = Delayed::Job.enqueue ErrorJob.new
    end

    it "records last_error when destroy_failed_jobs = false, max_attempts = 1" do
      Delayed::Worker.on_max_failures = proc { false }
      @job.max_attempts = 1
      @job.save!
      expect(job = Delayed::Job.get_and_lock_next_available("w1")).to eq(@job)
      @worker.perform(job)
      old_id = @job.id
      @job = Delayed::Job.list_jobs(:failed, 1).first
      expect(@job.original_job_id).to eq(old_id)
      expect(@job.last_error).to match(/did not work/)
      expect(@job.last_error).to match(%r{shared/worker.rb})
      expect(@job.attempts).to eq(1)
      expect(@job.failed_at).not_to be_nil
      expect(@job.run_at).to be > Delayed::Job.db_time_now - 10.minutes
      expect(@job.run_at).to be < Delayed::Job.db_time_now + 10.minutes
      # job stays locked after failing, for record keeping of time/worker
      expect(@job).to be_locked

      expect(Delayed::Job.find_available(100, @job.queue)).to eq([])
    end

    it "re-schedules jobs after failing" do
      @worker.perform(@job)
      @job = Delayed::Job.find(@job.id)
      expect(@job.last_error).to match(/did not work/)
      expect(@job.last_error).to match(/sample_jobs.rb:22:in `perform'/)
      expect(@job.attempts).to eq(1)
      expect(@job.run_at).to be > Delayed::Job.db_time_now - 10.minutes
      expect(@job.run_at).to be < Delayed::Job.db_time_now + 10.minutes
    end

    it "accepts :unlock return value from on_failure during reschedule and unlock the job" do
      expect_any_instance_of(Delayed::Job).to receive(:unlock).once
      @job = Delayed::Job.enqueue(UnlockJob.new(1))
      @worker.perform(@job)
    end

    it "notifies jobs on failure" do
      ErrorJob.failure_runs = 0
      @worker.perform(@job)
      expect(ErrorJob.failure_runs).to eq(1)
    end

    it "notifies jobs on permanent failure" do
      (Delayed::Settings.max_attempts - 1).times { @job.reschedule }
      ErrorJob.permanent_failure_runs = 0
      @worker.perform(@job)
      expect(ErrorJob.permanent_failure_runs).to eq(1)
    end
  end

  context "reschedule" do
    before do
      @job = Delayed::Job.create payload_object: SimpleJob.new
    end

    context "and we want to destroy jobs" do
      it "is destroyed if it failed more than Settings.max_attempts times" do
        expect(@job).to receive(:destroy)
        Delayed::Settings.max_attempts.times { @job.reschedule }
      end

      it "is not destroyed if failed fewer than Settings.max_attempts times" do
        expect(@job).not_to receive(:destroy)
        (Delayed::Settings.max_attempts - 1).times { @job.reschedule }
      end

      it "is destroyed if failed more than Job#max_attempts times" do
        Delayed::Settings.max_attempts = 25
        expect(@job).to receive(:destroy)
        @job.max_attempts = 2
        @job.save!
        2.times { @job.reschedule }
      end

      it "is destroyed if it has expired" do
        job = Delayed::Job.create payload_object: SimpleJob.new, expires_at: Delayed::Job.db_time_now - 1.day
        expect(job).to receive(:destroy)
        job.reschedule
      end
    end

    context "and we don't want to destroy jobs" do
      before do
        Delayed::Worker.on_max_failures = proc { false }
      end

      after do
        Delayed::Worker.on_max_failures = nil
      end

      it "is failed if it failed more than Settings.max_attempts times" do
        expect(@job.failed_at).to be_nil
        Delayed::Settings.max_attempts.times { @job.reschedule }
        expect(Delayed::Job.list_jobs(:failed, 100).size).to eq(1)
      end

      it "is not failed if it failed fewer than Settings.max_attempts times" do
        (Delayed::Settings.max_attempts - 1).times { @job.reschedule }
        @job = Delayed::Job.find(@job.id)
        expect(@job.failed_at).to be_nil
      end

      it "is failed if it has expired" do
        job = Delayed::Job.create payload_object: SimpleJob.new, expires_at: Delayed::Job.db_time_now - 1.day
        expect(job).to receive(:fail!)
        job.reschedule
      end
    end

    context "and we give an on_max_failures callback" do
      it "is failed max_attempts times and cb is false" do
        Delayed::Worker.on_max_failures = proc do |job, _ex|
          expect(job).to eq(@job)
          false
        end
        expect(@job).to receive(:fail!)
        Delayed::Settings.max_attempts.times { @job.reschedule }
      end

      it "is destroyed if it failed max_attempts times and cb is true" do
        Delayed::Worker.on_max_failures = proc do |job, _ex|
          expect(job).to eq(@job)
          true
        end
        expect(@job).to receive(:destroy)
        Delayed::Settings.max_attempts.times { @job.reschedule }
      end
    end
  end

  context "Queue workers" do
    before do
      Delayed::Settings.queue = "Queue workers test"
      job_create(queue: "queue1")
      job_create(queue: "queue2")
    end

    it "onlies work off jobs assigned to themselves" do
      worker = worker_create(queue: "queue1")
      expect(SimpleJob.runs).to eq(0)
      worker.run
      expect(SimpleJob.runs).to eq(1)

      SimpleJob.runs = 0

      worker = worker_create(queue: "queue2")
      expect(SimpleJob.runs).to eq(0)
      worker.run
      expect(SimpleJob.runs).to eq(1)
    end

    it "does not work off jobs not assigned to themselves" do
      worker = worker_create(queue: "queue3")

      expect(SimpleJob.runs).to eq(0)
      worker.run
      expect(SimpleJob.runs).to eq(0)
    end

    it "gets the default queue if none is set" do
      queue_name = "default_queue"
      Delayed::Settings.queue = queue_name
      worker = worker_create(queue: nil)
      expect(worker.queue_name).to eq(queue_name)
    end

    it "overrides default queue name if specified in initialize" do
      queue_name = "my_queue"
      Delayed::Settings.queue = "default_queue"
      worker = worker_create(queue: queue_name)
      expect(worker.queue_name).to eq(queue_name)
    end
  end

  context "plugins" do
    it "creates and call the plugin callbacks" do
      TestPlugin.runs = 0
      Delayed::Worker.plugins << TestPlugin
      job_create
      @worker = Delayed::Worker.new(quiet: true)
      @worker.run
      expect(TestPlugin.runs).to eq(1)
      expect(SimpleJob.runs).to eq(1)
    end
  end

  describe "expires_at" do
    it "runs non-expired jobs" do
      Delayed::Job.enqueue SimpleJob.new, expires_at: Delayed::Job.db_time_now + 1.day
      expect { @worker.run }.to change(SimpleJob, :runs).by(1)
    end

    it "does not run expired jobs" do
      Delayed::Job.enqueue SimpleJob.new, expires_at: Delayed::Job.db_time_now - 1.day
      expect { @worker.run }.not_to change(SimpleJob, :runs)
    end

    it "reports a permanent failure when an expired job is dequeued" do
      ErrorJob.last_error = nil
      Delayed::Job.enqueue ErrorJob.new, expires_at: Delayed::Job.db_time_now - 1.day
      expect { @worker.run }.to change(ErrorJob, :permanent_failure_runs).by(1)
      expect(ErrorJob.last_error).to be_a Delayed::Backend::JobExpired
    end
  end

  describe "delay failure callbacks" do
    it "calls the on_failure callback" do
      ErrorJob.last_error = nil
      ErrorJob.new.delay(max_attempts: 2, on_failure: :on_failure).perform
      expect { @worker.run }.to change(ErrorJob, :failure_runs).by(1)
      expect(ErrorJob.last_error.to_s).to eq "did not work"
    end

    it "calls the on_permanent_failure callback" do
      ErrorJob.last_error = nil
      ErrorJob.new.delay(max_attempts: 1, on_permanent_failure: :on_failure).perform
      expect { @worker.run }.to change(ErrorJob, :failure_runs).by(1)
      expect(ErrorJob.last_error.to_s).to eq "did not work"
    end
  end

  describe "custom deserialization errors" do
    it "reschedules with more attempts left" do
      job = Delayed::Job.create({ payload_object: DeserializeErrorJob.new, max_attempts: 2 })
      job.instance_variable_set("@payload_object", nil)
      worker = Delayed::Worker.new(max_priority: nil, min_priority: nil, quiet: true)
      expect { worker.perform(job) }.not_to raise_error
    end

    it "run permanent failure code on last attempt" do
      job = Delayed::Job.create({ payload_object: DeserializeErrorJob.new, max_attempts: 1 })
      job.instance_variable_set("@payload_object", nil)
      worker = Delayed::Worker.new(max_priority: nil, min_priority: nil, quiet: true)
      expect { worker.perform(job) }.not_to raise_error
    end
  end

  describe "#start" do
    it "fires off an execute callback on the processing jobs loop" do
      fired = false
      expect(@worker).to receive(:exit?).and_return(true)
      Delayed::Worker.lifecycle.before(:execute) { |w| w == @worker && fired = true }
      @worker.start
      expect(fired).to be(true)
    end
  end

  describe "#run" do
    it "fires off a loop callback on each call to run" do
      fired = 0
      Delayed::Worker.lifecycle.before(:loop) { |w| w == @worker && fired += 1 }
      expect(Delayed::Job).to receive(:get_and_lock_next_available).twice.and_return(nil)
      @worker.run
      @worker.run
      expect(fired).to eq(2)
    end
  end
end
