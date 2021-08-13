# frozen_string_literal: true

shared_examples_for "Delayed::Batch" do
  context "batching" do
    it "should batch up all deferrable delayed methods" do
      later = 1.hour.from_now
      Delayed::Batch.serial_batch do
        expect("string".delay(ignore_transaction: true).size).to be true
        # won't be batched, it'll get its own job
        expect("string".delay(run_at: later, ignore_transaction: true).reverse).to be_truthy
        expect("string".delay(ignore_transaction: true).gsub(/./, "!")).to be_truthy
      end
      batch_jobs = Delayed::Job.find_available(5)
      regular_jobs = Delayed::Job.list_jobs(:future, 5)
      expect(regular_jobs.size).to eq(1)
      expect(regular_jobs.first.batch?).to eq(false)
      expect(batch_jobs.size).to eq(1)
      batch_job = batch_jobs.first
      expect(batch_job.batch?).to eq(true)
      expect(batch_job.payload_object.mode).to eq(:serial)
      expect(batch_job.payload_object.jobs.map do |j|
               [j.payload_object.object, j.payload_object.method, j.payload_object.args]
             end).to eq([
                          [
                            "string", :size, []
                          ],
                          [
                            "string", :gsub, [
                              /./, "!"
                            ]
                          ]
                        ])
    end

    it "should not let you invoke it directly" do
      Delayed::Batch.serial_batch do
        expect("string".delay(ignore_transaction: true).size).to be true
        expect("string".delay(ignore_transaction: true).gsub(/./, "!")).to be true
      end
      expect(Delayed::Job.jobs_count(:current)).to eq(1)
      job = Delayed::Job.find_available(1).first
      expect { job.invoke_job }.to raise_error(RuntimeError)
    end

    it "should create valid jobs" do
      Delayed::Batch.serial_batch do
        expect("string".delay(ignore_transaction: true).size).to be true
        expect("string".delay(ignore_transaction: true).gsub(/./, "!")).to be true
      end
      expect(Delayed::Job.jobs_count(:current)).to eq(1)

      batch_job = Delayed::Job.find_available(1).first
      expect(batch_job.batch?).to eq(true)
      jobs = batch_job.payload_object.jobs
      expect(jobs.size).to eq(2)
      expect(jobs[0]).to be_new_record
      expect(jobs[0].payload_object.class).to   eq(Delayed::PerformableMethod)
      expect(jobs[0].payload_object.method).to  eq(:size)
      expect(jobs[0].payload_object.args).to    eq([])
      expect(jobs[0].payload_object.perform).to eq(6)
      expect(jobs[1]).to be_new_record
      expect(jobs[1].payload_object.class).to   eq(Delayed::PerformableMethod)
      expect(jobs[1].payload_object.method).to  eq(:gsub)
      expect(jobs[1].payload_object.args).to    eq([/./, "!"])
      expect(jobs[1].payload_object.perform).to eq("!!!!!!")
    end

    it "should create a different batch for each priority" do
      Delayed::Batch.serial_batch do
        expect("string".delay(priority: Delayed::LOW_PRIORITY, ignore_transaction: true).size).to be true
        expect("string".delay(ignore_transaction: true).gsub(/./, "!")).to be true
      end
      expect(Delayed::Job.jobs_count(:current)).to eq(2)
    end

    it "should use the given priority for all, if specified" do
      Delayed::Batch.serial_batch(priority: 11) do
        expect("string".delay(priority: 20, ignore_transaction: true).size).to be true
        expect("string".delay(priority: 15, ignore_transaction: true).gsub(/./, "!")).to be true
      end
      expect(Delayed::Job.jobs_count(:current)).to eq(1)
      expect(Delayed::Job.find_available(1).first.priority).to eq(11)
    end

    it "should just create the job, if there's only one in the batch" do
      Delayed::Batch.serial_batch(priority: 11) do
        expect("string".delay(ignore_transaction: true).size).to be true
      end
      expect(Delayed::Job.jobs_count(:current)).to eq(1)
      expect(Delayed::Job.find_available(1).first.tag).to eq("String#size")
      expect(Delayed::Job.find_available(1).first.priority).to eq(11)
    end

    it "should list a job only once when the same call is made multiple times" do
      Delayed::Batch.serial_batch(priority: 11) do
        "string".delay(ignore_transaction: true).size
        "string".delay(ignore_transaction: true).gsub(/./, "!")
        "string".delay(ignore_transaction: true).size
      end
      batch_job = Delayed::Job.find_available(1).first
      jobs = batch_job.payload_object.jobs
      expect(jobs.size).to eq(2)
    end
  end
end
