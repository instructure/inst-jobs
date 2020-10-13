shared_examples_for 'Delayed::Batch' do
  context "batching" do
    it "should batch up all deferrable delayed methods" do
      later = 1.hour.from_now
      Delayed::Batch.serial_batch {
        "string".delay(ignore_transaction: true).size.should be true
        "string".delay(run_at: later, ignore_transaction: true).reverse.should be_truthy # won't be batched, it'll get its own job
        "string".delay(ignore_transaction: true).gsub(/./, "!").should be_truthy
      }
      batch_jobs = Delayed::Job.find_available(5)
      regular_jobs = Delayed::Job.list_jobs(:future, 5)
      regular_jobs.size.should == 1
      regular_jobs.first.batch?.should == false
      batch_jobs.size.should == 1
      batch_job = batch_jobs.first
      batch_job.batch?.should == true
      batch_job.payload_object.mode.should  == :serial
      batch_job.payload_object.jobs.map { |j| [j.payload_object.object, j.payload_object.method, j.payload_object.args] }.should  == [
        ["string", :size, []],
        ["string", :gsub, [/./, "!"]]
      ]
    end

    it "should not let you invoke it directly" do
      later = 1.hour.from_now
      Delayed::Batch.serial_batch {
        "string".delay(ignore_transaction: true).size.should be true
        "string".delay(ignore_transaction: true).gsub(/./, "!").should be true
      }
      Delayed::Job.jobs_count(:current).should == 1
      job = Delayed::Job.find_available(1).first
      expect{ job.invoke_job }.to raise_error(RuntimeError)
    end

    it "should create valid jobs" do
      Delayed::Batch.serial_batch {
        "string".delay(ignore_transaction: true).size.should be true
        "string".delay(ignore_transaction: true).gsub(/./, "!").should be true
      }
      Delayed::Job.jobs_count(:current).should == 1

      batch_job = Delayed::Job.find_available(1).first
      batch_job.batch?.should == true
      jobs = batch_job.payload_object.jobs
      jobs.size.should == 2
      jobs[0].should be_new_record
      jobs[0].payload_object.class.should   == Delayed::PerformableMethod
      jobs[0].payload_object.method.should  == :size
      jobs[0].payload_object.args.should    == []
      jobs[0].payload_object.perform.should == 6
      jobs[1].should be_new_record
      jobs[1].payload_object.class.should   == Delayed::PerformableMethod
      jobs[1].payload_object.method.should  == :gsub
      jobs[1].payload_object.args.should    == [/./, "!"]
      jobs[1].payload_object.perform.should == "!!!!!!"
    end

    it "should create a different batch for each priority" do
      later = 1.hour.from_now
      Delayed::Batch.serial_batch {
        "string".delay(priority: Delayed::LOW_PRIORITY, ignore_transaction: true).size.should be true
        "string".delay(ignore_transaction: true).gsub(/./, "!").should be true
      }
      Delayed::Job.jobs_count(:current).should == 2
    end

    it "should use the given priority for all, if specified" do
      Delayed::Batch.serial_batch(:priority => 11) {
        "string".delay(priority: 20, ignore_transaction: true).size.should be true
        "string".delay(priority: 15, ignore_transaction: true).gsub(/./, "!").should be true
      }
      Delayed::Job.jobs_count(:current).should == 1
      Delayed::Job.find_available(1).first.priority.should == 11
    end

    it "should just create the job, if there's only one in the batch" do
      Delayed::Batch.serial_batch(:priority => 11) {
        "string".delay(ignore_transaction: true).size.should be true
      }
      Delayed::Job.jobs_count(:current).should == 1
      Delayed::Job.find_available(1).first.tag.should == "String#size"
      Delayed::Job.find_available(1).first.priority.should == 11
    end

    it "should list a job only once when the same call is made multiple times" do
      Delayed::Batch.serial_batch(:priority => 11) {
        "string".delay(ignore_transaction: true).size
        "string".delay(ignore_transaction: true).gsub(/./, "!")
        "string".delay(ignore_transaction: true).size
      }
      batch_job = Delayed::Job.find_available(1).first
      jobs = batch_job.payload_object.jobs
      jobs.size.should == 2
    end
  end
end
