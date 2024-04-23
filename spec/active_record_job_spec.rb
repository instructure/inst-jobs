# frozen_string_literal: true

describe "Delayed::Backed::ActiveRecord::Job" do
  before :all do
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
  end

  before do
    Delayed::Testing.clear_all!
  end

  include_examples "a delayed_jobs implementation"

  it "recovers as well as possible from a failure failing a job" do
    allow(Delayed::Job::Failed).to receive(:create).and_raise(RuntimeError)
    job = "test".delay(ignore_transaction: true).reverse
    job_id = job.id
    expect { job.fail! }.to raise_error(RuntimeError)
    expect { Delayed::Job.find(job_id) }.to raise_error(ActiveRecord::RecordNotFound)
    expect(Delayed::Job.count).to eq(0)
  end

  context "when another worker has worked on a task since the job was found to be available, it" do
    before do
      @job = Delayed::Job.create payload_object: SimpleJob.new
      @job_copy_for_worker2 = Delayed::Job.find(@job.id)
    end

    it "does not allow a second worker to get exclusive access if already successfully processed by worker1" do
      @job.destroy
      expect(@job_copy_for_worker2.send(:lock_exclusively!, "worker2")).to be(false)
    end

    it "doesn't allow a second worker to get exclusive access if failed to be " \
       "processed by worker1 and run_at time is now in future (due to backing off behaviour)" do
      @job.update(attempts: 1, run_at: 1.day.from_now)
      expect(@job_copy_for_worker2.send(:lock_exclusively!, "worker2")).to be(false)
    end

    it "selects the next job at random if enabled" do
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

  it "unlocks a successfully locked job and persist the job's unlocked state" do
    job = Delayed::Job.create payload_object: SimpleJob.new
    expect(job.send(:lock_exclusively!, "worker1")).to be(true)
    job.reload
    job.unlock
    job.save!
    job.reload
    expect(job.locked_by).to be_nil
    expect(job.locked_at).to be_nil
  end

  describe "bulk_update failed jobs" do
    context "holding/unholding failed jobs" do
      before do
        @job = Delayed::Job.create payload_object: SimpleJob.new
        expect(Delayed::Job.get_and_lock_next_available("worker1")).to eq(@job)
        @job.fail!
      end

      it "raises error when holding failed jobs" do
        expect { Delayed::Job.bulk_update("hold", flavor: "failed", query: @query) }.to raise_error(RuntimeError)
      end

      it "raises error unholding failed jobs" do
        expect { Delayed::Job.bulk_update("unhold", flavor: "failed", query: @query) }.to raise_error(RuntimeError)
      end
    end

    context "deleting failed jobs" do
      before do
        2.times do
          j = Delayed::Job.create(payload_object: SimpleJob.new)
          expect(j.send(:lock_exclusively!, "worker1")).to be(true)
          j.fail!
        end
      end

      it "deletes failed jobs by id" do
        target_ids = Delayed::Job::Failed.all[0..2].map(&:id)
        expect(Delayed::Job.bulk_update("destroy",
                                        ids: target_ids,
                                        flavor: "failed",
                                        query: @query)).to eq(target_ids.length)
      end

      it "deletes all failed jobs" do
        failed_count = Delayed::Job::Failed.count
        expect(Delayed::Job.bulk_update("destroy", flavor: "failed", query: @query)).to eq(failed_count)
      end

      it "deletes all failed jobs before a given date" do
        Delayed::Job::Failed.first.update!(failed_at: 3.hours.ago)
        Delayed::Job::Failed.last.update!(failed_at: 1.hour.ago)

        expect(Delayed::Job::Failed.count).to eq 2
        Delayed::Job::Failed.cleanup_old_jobs(2.hours.ago)
        expect(Delayed::Job::Failed.count).to eq 1
      end
    end
  end

  context "n_strand" do
    it "defaults to 1" do
      expect(Delayed::Job).not_to receive(:rand)
      job = Delayed::Job.enqueue(SimpleJob.new, n_strand: "njobs")
      expect(job.strand).to eq("njobs")
    end

    it "sets max_concurrent based on num_strands" do
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
      it "uses the first param as the setting to read" do
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

      it "allows overridding the setting based on the second param" do
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
      it "sets one job as next_in_strand at a time with max_concurrent of 1" do
        job1 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
        job1.reload
        expect(job1.next_in_strand).to be(true)
        job2 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
        job2.reload
        expect(job2.next_in_strand).to be(false)
        run_job(job1)
        job2.reload
        expect(job2.next_in_strand).to be(true)
      end

      it "sets multiple jobs as next_in_strand at a time based on max_concurrent" do
        change_setting(Delayed::Settings, :num_strands, lambda { |strand_name|
          case strand_name
          when "njobs" then 2
          end
        }) do
          job1 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
          job1.reload
          expect(job1.next_in_strand).to be(true)
          job2 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
          job2.reload
          expect(job2.next_in_strand).to be(true)
          job3 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
          job3.reload
          expect(job3.next_in_strand).to be(false)
          run_job(job1)
          job3.reload
          expect(job3.next_in_strand).to be(true)
        end
      end
    end
  end

  it "unlocks orphaned prefetched_jobs" do
    job1 = Delayed::Job.new(tag: "tag")
    job2 = Delayed::Job.new(tag: "tag")

    job1.create_and_lock!("prefetch:a")
    job1.locked_at = Delayed::Job.db_time_now - (15 * 60)
    job1.save!
    job2.create_and_lock!("prefetch:a")

    expect(Delayed::Job.unlock_orphaned_prefetched_jobs).to eq 1
    expect(Delayed::Job.unlock_orphaned_prefetched_jobs).to eq 0

    expect(Delayed::Job.find(job1.id).locked_by).to be_nil
    expect(Delayed::Job.find(job2.id).locked_by).to eq "prefetch:a"
  end

  it "gets process ids from locked_by" do
    Array.new(3) { Delayed::Job.create payload_object: SimpleJob.new }
    Delayed::Job.get_and_lock_next_available(["job42:2", "job42:9001"])
    expect(Delayed::Job.processes_locked_locally(name: "job42").sort).to eq [2, 9001]
    expect(Delayed::Job.processes_locked_locally(name: "jobnotme")).to be_empty
  end

  it "allows fetching multiple jobs at once" do
    jobs = Array.new(3) { Delayed::Job.create payload_object: SimpleJob.new }
    locked_jobs = Delayed::Job.get_and_lock_next_available(%w[worker1 worker2])
    expect(locked_jobs.length).to eq(2)
    expect(locked_jobs.keys).to eq(%w[worker1 worker2])
    expect(locked_jobs.values).to eq(jobs[0..1])
    expect(jobs.map { |j| j.reload.locked_by }).to eq(["worker1", "worker2", nil])
  end

  it "allows fetching extra jobs" do
    jobs = Array.new(5) { Delayed::Job.create payload_object: SimpleJob.new }
    locked_jobs = Delayed::Job.get_and_lock_next_available(["worker1"],
                                                           prefetch: 2,
                                                           prefetch_owner: "work_queue")
    expect(locked_jobs.length).to eq 2
    expect(locked_jobs.keys).to eq %w[worker1 work_queue]
    expect(locked_jobs["worker1"]).to eq jobs[0]
    expect(locked_jobs["work_queue"]).to eq jobs[1..2]
    expect(jobs.map { |j| j.reload.locked_by }).to eq(["worker1", "work_queue", "work_queue", nil, nil])
  end

  it "does not find jobs scheduled for now when we have forced latency" do
    job = create_job
    expect(Delayed::Job.get_and_lock_next_available("worker", forced_latency: 60.0)).to be_nil
    expect(Delayed::Job.get_and_lock_next_available("worker")).to eq job
  end

  context "apply_temp_strand!" do
    it "applies strand" do
      5.times { "1".delay.to_i }
      scope = Delayed::Job.where(tag: "String#to_i")
      count, new_strand = Delayed::Job.apply_temp_strand!(scope, max_concurrent: 2)
      expect(count).to eq 5
      expect(Delayed::Job.where(strand: new_strand).count).to eq 5
      expect(Delayed::Job.where(strand: new_strand, next_in_strand: true).count).to eq 2
    end

    it "raises ArgumentError if job scope contains strand" do
      "1".delay(strand: "foo").to_i
      scope = Delayed::Job.where(tag: "String#to_i")
      expect { Delayed::Job.apply_temp_strand!(scope) }.to raise_error(ArgumentError)
    end

    it "raises ArgumentError if job scope contains singleton" do
      "1".delay(singleton: "baz").to_i
      scope = Delayed::Job.where(tag: "String#to_i")
      expect { Delayed::Job.apply_temp_strand!(scope) }.to raise_error(ArgumentError)
    end
  end

  context "non-transactional", :non_transactional do
    it "creates a stranded job in a single statement" do
      allow(Delayed::Job.connection).to receive(:prepared_statements).and_return(false)
      allow(Delayed::Job.connection).to receive(:execute).with(be_include("pg_advisory_xact_lock"),
                                                               anything).and_call_original.once
      expect(Delayed::Job.connection).not_to receive(:insert)
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
      expect(Delayed::Job.connection).not_to receive(:insert)
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
      expect(Delayed::Job.connection).not_to receive(:insert)
      j.fail!
      allow(Delayed::Job.connection).to receive(:execute).and_call_original
    end

    it "recovers gracefully when multiple singleton jobs have next_in_strand set to true" do
      j1 = create_job(singleton: "(te(\"')st)")
      expect(j1).not_to be_new_record
      expect(Delayed::Job.get_and_lock_next_available("worker")).to eq j1
      j2 = create_job(singleton: "(te(\"')st)")
      expect(j2).not_to be_new_record
      j2.reload
      expect(j1).not_to eq j2
      expect(j2).not_to be_next_in_strand
      # set the flag incorrectly, out-of-band
      j2.update!(next_in_strand: true)
      # j2 can't be locked yet, even though next_in_strand is true
      expect(Delayed::Job.get_and_lock_next_available("worker")).to be_nil
      # the invalid data was repaired
      expect(j2.reload).not_to be_next_in_strand
    end

    it "recovers gracefully when multiple singleton jobs have next_in_strand set to true, " \
       "and it finishes while fixing it" do
      j1 = create_job(singleton: "test")
      expect(j1).not_to be_new_record
      expect(Delayed::Job.get_and_lock_next_available("worker")).to eq j1
      j2 = create_job(singleton: "test")
      expect(j2).not_to be_new_record
      j2.reload
      expect(j1).not_to eq j2
      expect(j2).not_to be_next_in_strand
      # set the flag incorrectly, out-of-band
      j2.update!(next_in_strand: true)
      expect(Delayed::Job).to receive(:advisory_lock) do
        j1.destroy
      end
      # j1 will go away while we're trying to fix it, so we should be able to lock it
      expect(Delayed::Job.get_and_lock_next_available("worker")).to eq j2
      expect { j1.reload }.to raise_error(ActiveRecord::RecordNotFound)
    end
  end

  context "Failed#requeue!" do
    it "requeues a failed job" do
      j = create_job(attempts: 1, max_attempts: 1)
      fj = j.fail!

      lifecycle_args = nil
      Delayed::Worker.lifecycle.after(:create) do |args|
        lifecycle_args = args
      end
      j2 = fj.requeue!
      expect(lifecycle_args["payload_object"].class).to eq SimpleJob
      expect(fj.reload.requeued_job_id).to eq j2.id

      orig_atts = j.attributes.except("id", "locked_at", "locked_by")
      new_atts = j2.attributes.except("id", "locked_at", "locked_by")
      expect(orig_atts).to eq(new_atts)

      # ensure the requeued job actually runs even though `attempts` are maxed out
      # (so it will not be retried after being manually requeued)
      expect { run_job(j2) }.to change(SimpleJob, :runs).by(1)
    end
  end
end
