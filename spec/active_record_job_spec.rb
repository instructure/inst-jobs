# frozen_string_literal: true

describe "Delayed::Backed::ActiveRecord::Job" do
  before :all do
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
  end

  before do
    Delayed::Testing.clear_all!
  end

  include_examples "a delayed_jobs implementation"

  it "should recover as well as possible from a failure failing a job" do
    allow(Delayed::Job::Failed).to receive(:create).and_raise(RuntimeError)
    job = "test".delay(ignore_transaction: true).reverse
    job_id = job.id
    expect { job.fail! }.to raise_error(RuntimeError)
    expect { Delayed::Job.find(job_id) }.to raise_error(ActiveRecord::RecordNotFound)
    expect(Delayed::Job.count).to eq(0)
  end

  context "when another worker has worked on a task since the job was found to be available, it" do
    before :each do
      @job = Delayed::Job.create payload_object: SimpleJob.new
      @job_copy_for_worker2 = Delayed::Job.find(@job.id)
    end

    it "should not allow a second worker to get exclusive access if already successfully processed by worker1" do
      @job.destroy
      expect(@job_copy_for_worker2.send(:lock_exclusively!, "worker2")).to eq(false)
    end

    it "doesn't  allow a second worker to get exclusive access if failed to be processed by worker1 and
        run_at time is now in future (due to backing off behaviour)" do
      @job.update(attempts: 1, run_at: 1.day.from_now)
      expect(@job_copy_for_worker2.send(:lock_exclusively!, "worker2")).to eq(false)
    end

    it "should select the next job at random if enabled" do
      Delayed::Settings.select_random_from_batch = true
      15.times { "test".delay.length }
      founds = []
      15.times do
        job = Delayed::Job.get_and_lock_next_available("tester")
        founds << job
        job.unlock
        job.save!
      end
      expect(founds.uniq.size).to be > 1
    ensure
      Delayed::Settings.select_random_from_batch = false
    end
  end

  it "should unlock a successfully locked job and persist the job's unlocked state" do
    job = Delayed::Job.create payload_object: SimpleJob.new
    expect(job.send(:lock_exclusively!, "worker1")).to eq(true)
    job.reload
    job.unlock
    job.save!
    job.reload
    expect(job.locked_by).to eq(nil)
    expect(job.locked_at).to eq(nil)
  end

  describe "bulk_update failed jobs" do
    context "holding/unholding failed jobs" do
      before :each do
        @job = Delayed::Job.create payload_object: SimpleJob.new
        expect(Delayed::Job.get_and_lock_next_available("worker1")).to eq(@job)
        @job.fail!
      end

      it "should raise error when holding failed jobs" do
        expect { Delayed::Job.bulk_update("hold", flavor: "failed", query: @query) }.to raise_error(RuntimeError)
      end

      it "should raise error unholding failed jobs" do
        expect { Delayed::Job.bulk_update("unhold", flavor: "failed", query: @query) }.to raise_error(RuntimeError)
      end
    end

    context "deleting failed jobs" do
      before :each do
        2.times do
          j = Delayed::Job.create(payload_object: SimpleJob.new)
          expect(j.send(:lock_exclusively!, "worker1")).to eq(true)
          j.fail!
        end
      end

      it "should delete failed jobs by id" do
        target_ids = Delayed::Job::Failed.all[0..2].map(&:id)
        expect(Delayed::Job.bulk_update("destroy", ids: target_ids, flavor: "failed",
                                                   query: @query)).to eq(target_ids.length)
      end

      it "should delete all failed jobs" do
        failed_count = Delayed::Job::Failed.count
        expect(Delayed::Job.bulk_update("destroy", flavor: "failed", query: @query)).to eq(failed_count)
      end
    end
  end

  context "n_strand" do
    it "should default to 1" do
      expect(Delayed::Job).to receive(:rand).never
      job = Delayed::Job.enqueue(SimpleJob.new, n_strand: "njobs")
      expect(job.strand).to eq("njobs")
    end

    it "should set max_concurrent based on num_strands" do
      change_setting(Delayed::Settings, :num_strands, lambda { |strand_name|
                                                        expect(strand_name).to eql "njobs"
                                                        "3"
                                                      }) do
        job = Delayed::Job.enqueue(SimpleJob.new, n_strand: "njobs")
        expect(job.strand).to eq("njobs")
        expect(job.max_concurrent).to eq(3)
      end
    end

    context "with two parameters" do
      it "should use the first param as the setting to read" do
        job = Delayed::Job.enqueue(SimpleJob.new, n_strand: %w[njobs 123])
        expect(job.strand).to eq("njobs/123")
        change_setting(Delayed::Settings, :num_strands, lambda { |strand_name|
          case strand_name
          when "njobs" then 3
          end
        }) do
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: %w[njobs 123])
          expect(job.strand).to eq("njobs/123")
          expect(job.max_concurrent).to eq(3)
        end
      end

      it "should allow overridding the setting based on the second param" do
        change_setting(Delayed::Settings, :num_strands, lambda { |strand_name|
          case strand_name
          when "njobs/123" then 5
          end
        }) do
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: %w[njobs 123])
          expect(job.strand).to eq("njobs/123")
          expect(job.max_concurrent).to eq(5)
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: %w[njobs 456])
          expect(job.strand).to eq("njobs/456")
          expect(job.max_concurrent).to eq(1)
        end

        change_setting(Delayed::Settings, :num_strands, lambda { |strand_name|
          case strand_name
          when "njobs/123" then 5
          when "njobs" then 3
          end
        }) do
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: %w[njobs 123])
          expect(job.strand).to eq("njobs/123")
          expect(job.max_concurrent).to eq(5)
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: %w[njobs 456])
          expect(job.strand).to eq("njobs/456")
          expect(job.max_concurrent).to eq(3)
        end
      end
    end

    context "max_concurrent triggers" do
      it "should set one job as next_in_strand at a time with max_concurrent of 1" do
        job1 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
        job1.reload
        expect(job1.next_in_strand).to eq(true)
        job2 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
        job2.reload
        expect(job2.next_in_strand).to eq(false)
        run_job(job1)
        job2.reload
        expect(job2.next_in_strand).to eq(true)
      end

      it "should set multiple jobs as next_in_strand at a time based on max_concurrent" do
        change_setting(Delayed::Settings, :num_strands, lambda { |strand_name|
          case strand_name
          when "njobs" then 2
          end
        }) do
          job1 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
          job1.reload
          expect(job1.next_in_strand).to eq(true)
          job2 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
          job2.reload
          expect(job2.next_in_strand).to eq(true)
          job3 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
          job3.reload
          expect(job3.next_in_strand).to eq(false)
          run_job(job1)
          job3.reload
          expect(job3.next_in_strand).to eq(true)
        end
      end
    end
  end

  it "unlocks orphaned prefetched_jobs" do
    job1 = Delayed::Job.new(tag: "tag")
    job2 = Delayed::Job.new(tag: "tag")

    job1.create_and_lock!("prefetch:a")
    job1.locked_at = Delayed::Job.db_time_now - 15 * 60
    job1.save!
    job2.create_and_lock!("prefetch:a")

    expect(Delayed::Job.unlock_orphaned_prefetched_jobs).to eq 1
    expect(Delayed::Job.unlock_orphaned_prefetched_jobs).to eq 0

    expect(Delayed::Job.find(job1.id).locked_by).to be_nil
    expect(Delayed::Job.find(job2.id).locked_by).to eq "prefetch:a"
  end

  it "gets process ids from locked_by" do
    3.times.map { Delayed::Job.create payload_object: SimpleJob.new }
    Delayed::Job.get_and_lock_next_available(["job42:2", "job42:9001"])
    expect(Delayed::Job.processes_locked_locally(name: "job42").sort).to eq [2, 9001]
    expect(Delayed::Job.processes_locked_locally(name: "jobnotme")).to be_empty
  end

  it "allows fetching multiple jobs at once" do
    jobs = 3.times.map { Delayed::Job.create payload_object: SimpleJob.new }
    locked_jobs = Delayed::Job.get_and_lock_next_available(%w[worker1 worker2])
    expect(locked_jobs.length).to eq(2)
    expect(locked_jobs.keys).to eq(%w[worker1 worker2])
    expect(locked_jobs.values).to eq(jobs[0..1])
    expect(jobs.map(&:reload).map(&:locked_by)).to eq(["worker1", "worker2", nil])
  end

  it "allows fetching extra jobs" do
    jobs = 5.times.map { Delayed::Job.create payload_object: SimpleJob.new }
    locked_jobs = Delayed::Job.get_and_lock_next_available(["worker1"],
                                                           prefetch: 2,
                                                           prefetch_owner: "work_queue")
    expect(locked_jobs.length).to eq 2
    expect(locked_jobs.keys).to eq %w[worker1 work_queue]
    expect(locked_jobs["worker1"]).to eq jobs[0]
    expect(locked_jobs["work_queue"]).to eq jobs[1..2]
    expect(jobs.map(&:reload).map(&:locked_by)).to eq(["worker1", "work_queue", "work_queue", nil, nil])
  end

  it "should not find jobs scheduled for now when we have forced latency" do
    job = create_job
    expect(Delayed::Job.get_and_lock_next_available("worker", forced_latency: 60.0)).to be_nil
    expect(Delayed::Job.get_and_lock_next_available("worker")).to eq job
  end

  context "non-transactional", non_transactional: true do
    it "creates a stranded job in a single statement" do
      allow(Delayed::Job.connection).to receive(:prepared_statements).and_return(false)
      allow(Delayed::Job.connection).to receive(:execute).with(be_include("pg_advisory_xact_lock"),
                                                               anything).and_call_original.once
      allow(Delayed::Job.connection).to receive(:insert).never
      j = create_job(strand: "test1")
      allow(Delayed::Job.connection).to receive(:execute).and_call_original
      expect(Delayed::Job.find(j.id)).to eq j
    end

    it "creates a non-stranded job in a single statement" do
      allow(Delayed::Job.connection).to receive(:prepared_statements).and_return(false)
      call_count = 0
      allow(Delayed::Job.connection).to receive(:execute).and_wrap_original do |m, (arg1, arg2)|
        call_count += 1
        m.call(arg1, arg2)
      end
      allow(Delayed::Job.connection).to receive(:insert).never
      j = create_job(strand: "test1")
      expect(call_count).to eq 1
      expect(Delayed::Job.find(j.id)).to eq j
    end

    it "does not lock a stranded failed job creation" do
      j = create_job(strand: "test1")
      # query for metadata to ensure it's loaded before we start mucking with the connection
      Delayed::Backend::ActiveRecord::Job::Failed.new

      allow(Delayed::Job.connection).to receive(:prepared_statements).and_return(false)
      allow(Delayed::Job.connection).to receive(:execute).and_wrap_original do |original, *args|
        expect(args.first).not_to include("pg_advisory_xact_lock")
        original.call(*args)
      end
      allow(Delayed::Job.connection).to receive(:insert).never
      j.fail!
      allow(Delayed::Job.connection).to receive(:execute).and_call_original
    end
  end
end
