shared_examples_for 'Delayed::Worker' do
  def job_create(opts = {})
    Delayed::Job.create({:payload_object => SimpleJob.new, :queue => Delayed::Settings.queue}.merge(opts))
  end
  def worker_create(opts = {})
    Delayed::Worker.new(opts.merge(:max_priority => nil, :min_priority => nil, :quiet => true))
  end

  before(:each) do
    @worker = worker_create
    SimpleJob.runs = 0
    Delayed::Worker.on_max_failures = nil
    Delayed::Settings.sleep_delay = ->{ 0.01 }
  end

  describe "running a job" do
    it "should not fail when running a job with a % in the name" do
      @job = "Some % Name here".send_later_enqueue_args(:starts_with?, { no_delay: true }, "Some % Name")
      @worker.perform(@job)
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

      it "should run each job in order" do
        bar = "bar"
        expect(bar).to receive(:scan).with("b").ordered
        expect(bar).to receive(:scan).with("a").ordered
        expect(bar).to receive(:scan).with("r").ordered
        batch = Delayed::Batch::PerformableBatch.new(:serial, [
          { :payload_object => Delayed::PerformableMethod.new(bar, :scan, ["b"]) },
          { :payload_object => Delayed::PerformableMethod.new(bar, :scan, ["a"]) },
          { :payload_object => Delayed::PerformableMethod.new(bar, :scan, ["r"]) },
        ])

        batch_job = Delayed::Job.create :payload_object => batch
        @worker.perform(batch_job).should == 3
        expect(@runs).to eql 4 # batch, plus all jobs
      end

      it "should succeed regardless of the success/failure of its component jobs" do
        change_setting(Delayed::Settings, :max_attempts, 2) do
          batch = Delayed::Batch::PerformableBatch.new(:serial, [
            { :payload_object => Delayed::PerformableMethod.new("foo", :reverse, []) },
            { :payload_object => Delayed::PerformableMethod.new(1, :/, [0]) },
            { :payload_object => Delayed::PerformableMethod.new("bar", :scan, ["r"]) },
          ])
          batch_job = Delayed::Job.create :payload_object => batch

          @worker.perform(batch_job).should == 3
          expect(@runs).to eql 3 # batch, plus two successful jobs

          to_retry = Delayed::Job.list_jobs(:future, 100)
          to_retry.size.should eql 1
          to_retry[0].payload_object.method.should eql :/
          to_retry[0].last_error.should =~ /divided by 0/
          to_retry[0].attempts.should == 1
        end
      end

      it "should retry a failed individual job" do
        batch = Delayed::Batch::PerformableBatch.new(:serial, [
          { :payload_object => Delayed::PerformableMethod.new(1, :/, [0]) },
        ])
        batch_job = Delayed::Job.create :payload_object => batch

        expect_any_instance_of(Delayed::Job).to receive(:reschedule).once
        @worker.perform(batch_job).should == 1
        expect(@runs).to eql 1 # just the batch
      end
    end
  end

  context "worker prioritization" do
    before(:each) do
      @worker = Delayed::Worker.new(:max_priority => 5, :min_priority => 2, :quiet => true)
    end

    it "should only run jobs that are >= min_priority" do
      SimpleJob.runs.should == 0

      job_create(:priority => 1)
      job_create(:priority => 3)
      @worker.run

      SimpleJob.runs.should == 1
    end

    it "should only run jobs that are <= max_priority" do
      SimpleJob.runs.should == 0

      job_create(:priority => 10)
      job_create(:priority => 4)

      @worker.run

      SimpleJob.runs.should == 1
    end
  end

  context "while running with locked jobs" do
    it "should not run jobs locked by another worker" do
      job_create(:locked_by => 'other_worker', :locked_at => (Delayed::Job.db_time_now - 1.minutes))
      lambda { @worker.run }.should_not change { SimpleJob.runs }
    end

    it "should run open jobs" do
      job_create
      lambda { @worker.run }.should change { SimpleJob.runs }.from(0).to(1)
    end
  end

  describe "failed jobs" do
    before do
      # reset defaults
      Delayed::Settings.max_attempts = 25
      @job = Delayed::Job.enqueue ErrorJob.new
    end

    it "should record last_error when destroy_failed_jobs = false, max_attempts = 1" do
      Delayed::Worker.on_max_failures = proc { false }
      @job.max_attempts = 1
      @job.save!
      (job = Delayed::Job.get_and_lock_next_available('w1')).should == @job
      @worker.perform(job)
      old_id = @job.id
      @job = Delayed::Job.list_jobs(:failed, 1).first
      @job.original_job_id.should == old_id
      @job.last_error.should =~ /did not work/
      @job.last_error.should =~ /shared\/worker.rb/
      @job.attempts.should == 1
      @job.failed_at.should_not be_nil
      @job.run_at.should > Delayed::Job.db_time_now - 10.minutes
      @job.run_at.should < Delayed::Job.db_time_now + 10.minutes
      # job stays locked after failing, for record keeping of time/worker
      @job.should be_locked

      Delayed::Job.find_available(100, @job.queue).should == []
    end

    it "should re-schedule jobs after failing" do
      @worker.perform(@job)
      @job = Delayed::Job.find(@job.id)
      @job.last_error.should =~ /did not work/
      @job.last_error.should =~ /sample_jobs.rb:8:in `perform'/
      @job.attempts.should == 1
      @job.run_at.should > Delayed::Job.db_time_now - 10.minutes
      @job.run_at.should < Delayed::Job.db_time_now + 10.minutes
    end

    it "should accept :unlock return value from on_failure during reschedule and unlock the job" do
      expect_any_instance_of(Delayed::Job).to receive(:unlock).once
      @job = Delayed::Job.enqueue(UnlockJob.new(1))
      @worker.perform(@job)
    end

    it "should notify jobs on failure" do
      ErrorJob.failure_runs = 0
      @worker.perform(@job)
      ErrorJob.failure_runs.should == 1
    end

    it "should notify jobs on permanent failure" do
      (Delayed::Settings.max_attempts - 1).times { @job.reschedule }
      ErrorJob.permanent_failure_runs = 0
      @worker.perform(@job)
      ErrorJob.permanent_failure_runs.should == 1
    end
  end

  context "reschedule" do
    before do
      @job = Delayed::Job.create :payload_object => SimpleJob.new
    end

    context "and we want to destroy jobs" do
      it "should be destroyed if it failed more than Settings.max_attempts times" do
        expect(@job).to receive(:destroy)
        Delayed::Settings.max_attempts.times { @job.reschedule }
      end

      it "should not be destroyed if failed fewer than Settings.max_attempts times" do
        expect(@job).to receive(:destroy).never
        (Delayed::Settings.max_attempts - 1).times { @job.reschedule }
      end

      it "should be destroyed if failed more than Job#max_attempts times" do
        Delayed::Settings.max_attempts = 25
        expect(@job).to receive(:destroy)
        @job.max_attempts = 2
        @job.save!
        2.times { @job.reschedule }
      end

      it "should be destroyed if it has expired" do
        job = Delayed::Job.create :payload_object => SimpleJob.new, :expires_at => Delayed::Job.db_time_now - 1.day
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

      it "should be failed if it failed more than Settings.max_attempts times" do
        @job.failed_at.should == nil
        Delayed::Settings.max_attempts.times { @job.reschedule }
        Delayed::Job.list_jobs(:failed, 100).size.should == 1
      end

      it "should not be failed if it failed fewer than Settings.max_attempts times" do
        (Delayed::Settings.max_attempts - 1).times { @job.reschedule }
        @job = Delayed::Job.find(@job.id)
        @job.failed_at.should == nil
      end

      it "should be failed if it has expired" do
        job = Delayed::Job.create :payload_object => SimpleJob.new, :expires_at => Delayed::Job.db_time_now - 1.day
        expect(job).to receive(:fail!)
        job.reschedule
      end
    end

    context "and we give an on_max_failures callback" do
      it "should be failed max_attempts times and cb is false" do
        Delayed::Worker.on_max_failures = proc do |job, ex|
          job.should == @job
          false
        end
        expect(@job).to receive(:fail!)
        Delayed::Settings.max_attempts.times { @job.reschedule }
      end

      it "should be destroyed if it failed max_attempts times and cb is true" do
        Delayed::Worker.on_max_failures = proc do |job, ex|
          job.should == @job
          true
        end
        expect(@job).to receive(:destroy)
        Delayed::Settings.max_attempts.times { @job.reschedule }
      end
    end
  end


  context "Queue workers" do
    before :each do
      Delayed::Settings.queue = "Queue workers test"
      job_create(:queue => 'queue1')
      job_create(:queue => 'queue2')
    end

    it "should only work off jobs assigned to themselves" do
      worker = worker_create(:queue=>'queue1')
      SimpleJob.runs.should == 0
      worker.run
      SimpleJob.runs.should == 1

      SimpleJob.runs = 0

      worker = worker_create(:queue=>'queue2')
      SimpleJob.runs.should == 0
      worker.run
      SimpleJob.runs.should == 1
    end

    it "should not work off jobs not assigned to themselves" do
      worker = worker_create(:queue=>'queue3')

      SimpleJob.runs.should == 0
      worker.run
      SimpleJob.runs.should == 0
    end

    it "should get the default queue if none is set" do
      queue_name = "default_queue"
      Delayed::Settings.queue = queue_name
      worker = worker_create(:queue=>nil)
      worker.queue_name.should == queue_name
    end

    it "should override default queue name if specified in initialize" do
      queue_name = "my_queue"
      Delayed::Settings.queue = "default_queue"
      worker = worker_create(:queue=>queue_name)
      worker.queue_name.should == queue_name
    end
  end

  context "plugins" do
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

    it "should create and call the plugin callbacks" do
      TestPlugin.runs = 0
      Delayed::Worker.plugins << TestPlugin
      job_create
      @worker = Delayed::Worker.new(:quiet => true)
      @worker.run
      expect(TestPlugin.runs).to eq(1)
      expect(SimpleJob.runs).to eq(1)
    end
  end

  describe "expires_at" do
    it "should run non-expired jobs" do
      Delayed::Job.enqueue SimpleJob.new, :expires_at => Delayed::Job.db_time_now + 1.day
      expect { @worker.run }.to change { SimpleJob.runs }.by(1)
    end

    it "should not run expired jobs" do
      Delayed::Job.enqueue SimpleJob.new, :expires_at => Delayed::Job.db_time_now - 1.day
      expect { @worker.run }.to change { SimpleJob.runs }.by(0)
    end

    it "should report a permanent failure when an expired job is dequeued" do
      ErrorJob.last_error = nil
      Delayed::Job.enqueue ErrorJob.new, :expires_at => Delayed::Job.db_time_now - 1.day
      expect { @worker.run }.to change { ErrorJob.permanent_failure_runs }.by(1)
      expect(ErrorJob.last_error).to be_a Delayed::Backend::JobExpired
    end
  end

  describe "send_later_enqueue_args failure callbacks" do
    it "should call the on_failure callback" do
      ErrorJob.last_error = nil
      ErrorJob.new.send_later_enqueue_args(:perform, :max_attempts => 2, :on_failure => :on_failure)
      expect { @worker.run }.to change { ErrorJob.failure_runs }.by(1)
      expect(ErrorJob.last_error.to_s).to eq 'did not work'
    end

    it "should call the on_permanent_failure callback" do
      ErrorJob.last_error = nil
      ErrorJob.new.send_later_enqueue_args(:perform, :max_attempts => 1, :on_permanent_failure => :on_failure)
      expect { @worker.run }.to change { ErrorJob.failure_runs }.by(1)
      expect(ErrorJob.last_error.to_s).to eq 'did not work'
    end
  end

  describe "custom deserialization errors" do
    it "should reschedule with more attempts left" do
      job = Delayed::Job.create({:payload_object => DeserializeErrorJob.new, max_attempts: 2})
      job.instance_variable_set("@payload_object", nil)
      worker = Delayed::Worker.new(:max_priority => nil, :min_priority => nil, :quiet => true)
      expect { worker.perform(job) }.not_to raise_error
    end

    it "run permanent failure code on last attempt" do
      job = Delayed::Job.create({:payload_object => DeserializeErrorJob.new, max_attempts: 1})
      job.instance_variable_set("@payload_object", nil)
      worker = Delayed::Worker.new(:max_priority => nil, :min_priority => nil, :quiet => true)
      expect { worker.perform(job) }.not_to raise_error
    end
  end

  describe "#start" do
    it "fires off an execute callback on the processing jobs loop" do
      fired = false
      expect(@worker).to receive(:exit?).and_return(true)
      Delayed::Worker.lifecycle.before(:execute) { |w| w == @worker && fired = true }
      @worker.start
      expect(fired).to eq(true)
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
