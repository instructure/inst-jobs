# frozen_string_literal: true

require "timeout"

module InDelayedJobTest
  def self.check_in_job
    Delayed::Job.in_delayed_job?.should == true
  end
end

def no_op_callback(_); end

shared_examples_for "a backend" do
  def create_job(opts = {})
    Delayed::Job.enqueue(SimpleJob.new, queue: nil, **opts)
  end

  before do
    SimpleJob.runs = 0
  end

  it "sets run_at automatically if not set" do
    expect(Delayed::Job.create(payload_object: ErrorJob.new).run_at).not_to be_nil
  end

  it "does not set run_at automatically if already set" do
    later = Delayed::Job.db_time_now + 5.minutes
    expect(Delayed::Job.create(payload_object: ErrorJob.new, run_at: later).run_at).to be_within(1).of(later)
  end

  it "raises ArgumentError when handler doesn't respond_to :perform" do
    expect { Delayed::Job.enqueue(Object.new) }.to raise_error(ArgumentError)
  end

  it "increases count after enqueuing items" do
    Delayed::Job.enqueue SimpleJob.new
    expect(Delayed::Job.jobs_count(:current)).to eq(1)
  end

  it "triggers the lifecycle event around the create" do
    called = false
    created_job = nil

    Delayed::Worker.lifecycle.after(:create) do |_, result:|
      called = true
      created_job = result
    end

    job = SimpleJob.new
    Delayed::Job.enqueue(job)

    expect(called).to be_truthy
    expect(created_job).to be_a Delayed::Job
    expect(created_job.tag).to eq "SimpleJob#perform"
  end

  it "doesn't fail when `after` callback method is missing `result:` parameter" do
    Delayed::Worker.lifecycle.after(:create, &method(:no_op_callback)) # rubocop:disable Performance/MethodObjectAsBlock

    expect { Delayed::Job.enqueue(SimpleJob.new) }.not_to raise_error
  end

  it "is able to set priority when enqueuing items" do
    @job = Delayed::Job.enqueue SimpleJob.new, priority: 5
    expect(@job.priority).to eq(5)
  end

  it "uses the default priority when enqueuing items" do
    Delayed::Job.default_priority = 0
    @job = Delayed::Job.enqueue SimpleJob.new
    expect(@job.priority).to eq(0)
    Delayed::Job.default_priority = 10
    @job = Delayed::Job.enqueue SimpleJob.new
    expect(@job.priority).to eq(10)
    Delayed::Job.default_priority = 0
  end

  it "is able to set run_at when enqueuing items" do
    later = Delayed::Job.db_time_now + 5.minutes
    @job = Delayed::Job.enqueue SimpleJob.new, priority: 5, run_at: later
    expect(@job.run_at).to be_within(1).of(later)
  end

  it "disallows setting a run_at with a strand" do
    later = Delayed::Job.db_time_now + 15.minutes
    expect { Delayed::Job.enqueue(SimpleJob.new, run_at: later, strand: "stranded") }
      .to raise_error(ArgumentError)
  end

  it "gives some grace for near-future run_at with a strand" do
    later = Delayed::Job.db_time_now + 5.seconds
    job = Delayed::Job.enqueue(SimpleJob.new, run_at: later, strand: "stranded")
    expect(job.run_at).to be_within(1).of(later)
  end

  it "is able to set expires_at when enqueuing items" do
    later = Delayed::Job.db_time_now + 1.day
    @job = Delayed::Job.enqueue SimpleJob.new, expires_at: later
    expect(@job.expires_at).to be_within(1).of(later)
  end

  it "works with jobs in modules" do
    M::ModuleJob.runs = 0
    job = Delayed::Job.enqueue M::ModuleJob.new
    expect { job.invoke_job }.to change(M::ModuleJob, :runs).from(0).to(1)
  end

  it "raises an DeserializationError when the job class is totally unknown" do
    job = Delayed::Job.new handler: "--- !ruby/object:JobThatDoesNotExist {}"
    expect { job.payload_object.perform }.to raise_error(Delayed::Backend::DeserializationError)
  end

  it "tries to load the class when it is unknown at the time of the deserialization" do
    job = Delayed::Job.new handler: "--- !ruby/object:JobThatDoesNotExist {}"
    expect { job.payload_object.perform }.to raise_error(Delayed::Backend::DeserializationError)
  end

  it "tries include the namespace when loading unknown objects" do
    job = Delayed::Job.new handler: "--- !ruby/object:Delayed::JobThatDoesNotExist {}"
    expect { job.payload_object.perform }.to raise_error(Delayed::Backend::DeserializationError)
  end

  it "alsoes try to load structs when they are unknown (raises TypeError)" do
    job = Delayed::Job.new handler: "--- !ruby/struct:JobThatDoesNotExist {}"
    expect { job.payload_object.perform }.to raise_error(Delayed::Backend::DeserializationError)
  end

  it "tries include the namespace when loading unknown structs" do
    job = Delayed::Job.new handler: "--- !ruby/struct:Delayed::JobThatDoesNotExist {}"
    expect { job.payload_object.perform }.to raise_error(Delayed::Backend::DeserializationError)
  end

  it "raises an DeserializationError when the handler is invalid YAML" do
    job = Delayed::Job.new handler: %(test: ""11")
    expect { job.payload_object.perform }.to raise_error(Delayed::Backend::DeserializationError, /parsing error/)
  end

  describe "find_available" do
    it "does not find failed jobs" do
      @job = create_job attempts: 50
      @job.fail!
      expect(Delayed::Job.find_available(5)).not_to include(@job)
    end

    it "does not find jobs scheduled for the future" do
      @job = create_job run_at: (Delayed::Job.db_time_now + 1.minute)
      expect(Delayed::Job.find_available(5)).not_to include(@job)
    end

    it "does not find jobs locked by another worker" do
      @job = create_job
      expect(Delayed::Job.get_and_lock_next_available("other_worker")).to eq(@job)
      expect(Delayed::Job.find_available(5)).not_to include(@job)
    end

    it "finds open jobs" do
      @job = create_job
      expect(Delayed::Job.find_available(5)).to include(@job)
    end

    it "returns an empty hash when asking for multiple jobs, and there aren't any" do
      locked_jobs = Delayed::Job.get_and_lock_next_available(%w[worker1 worker2])
      expect(locked_jobs).to eq({})
    end
  end

  context "when another worker is already performing an task, it" do
    before do
      @job = Delayed::Job.create payload_object: SimpleJob.new
      expect(Delayed::Job.get_and_lock_next_available("worker1")).to eq(@job)
    end

    it "does not allow a second worker to get exclusive access" do
      expect(Delayed::Job.get_and_lock_next_available("worker2")).to be_nil
    end

    it "is not found by another worker" do
      expect(Delayed::Job.find_available(1).length).to eq(0)
    end
  end

  describe "#name" do
    it "is the class name of the job that was enqueued" do
      expect(Delayed::Job.create(payload_object: ErrorJob.new).name).to eq("ErrorJob")
    end

    it "is the method that will be called if its a performable method object" do
      @job = Story.delay(ignore_transaction: true).create
      expect(@job.name).to eq("Story.create")
    end

    it "is the instance method that will be called if its a performable method object" do
      @job = Story.create(text: "...").delay(ignore_transaction: true).save
      expect(@job.name).to eq("Story#save")
    end
  end

  context "worker prioritization" do
    it "fetches jobs ordered by priority" do
      10.times { create_job priority: rand(10) }
      jobs = Delayed::Job.find_available(10)
      expect(jobs.size).to eq(10)
      jobs.each_cons(2) do |a, b|
        expect(a.priority).to be <= b.priority
      end
    end

    it "does not find jobs lower than the given priority" do
      create_job priority: 5
      found = Delayed::Job.get_and_lock_next_available("test1", Delayed::Settings.queue, 10, 20)
      expect(found).to be_nil
      job2 = create_job priority: 10
      found = Delayed::Job.get_and_lock_next_available("test1", Delayed::Settings.queue, 10, 20)
      expect(found).to eq(job2)
      job3 = create_job priority: 15
      found = Delayed::Job.get_and_lock_next_available("test2", Delayed::Settings.queue, 10, 20)
      expect(found).to eq(job3)
    end

    it "does not find jobs higher than the given priority" do
      create_job priority: 25
      found = Delayed::Job.get_and_lock_next_available("test1", Delayed::Settings.queue, 10, 20)
      expect(found).to be_nil
      job2 = create_job priority: 20
      found = Delayed::Job.get_and_lock_next_available("test1", Delayed::Settings.queue, 10, 20)
      expect(found).to eq(job2)
      job3 = create_job priority: 15
      found = Delayed::Job.get_and_lock_next_available("test2", Delayed::Settings.queue, 10, 20)
      expect(found).to eq(job3)
    end
  end

  context "clear_locks!" do
    before do
      @job = create_job(locked_by: "worker", locked_at: Delayed::Job.db_time_now)
    end

    it "clears locks for the given worker" do
      Delayed::Job.clear_locks!("worker")
      expect(Delayed::Job.find_available(5)).to include(@job)
    end

    it "does not clear locks for other workers" do
      Delayed::Job.clear_locks!("worker1")
      expect(Delayed::Job.find_available(5)).not_to include(@job)
    end
  end

  context "unlock" do
    before do
      @job = create_job(locked_by: "worker", locked_at: Delayed::Job.db_time_now)
    end

    it "clears locks" do
      @job.unlock
      expect(@job.locked_by).to be_nil
      expect(@job.locked_at).to be_nil
    end

    it "clears locks from multiple jobs" do
      job2 = create_job(locked_by: "worker", locked_at: Delayed::Job.db_time_now)
      Delayed::Job.unlock([@job, job2])
      expect(@job.locked_at).to be_nil
      expect(job2.locked_at).to be_nil
      # make sure it was persisted, too
      expect(Delayed::Job.find(@job.id).locked_at).to be_nil
    end
  end

  describe "#transfer_lock" do
    it "transfers lock" do
      job = create_job(locked_by: "worker", locked_at: Delayed::Job.db_time_now)
      expect(job.transfer_lock!(from: "worker", to: "worker2")).to be true
      expect(Delayed::Job.find(job.id).locked_by).to eq "worker2"
    end
  end

  context "strands" do
    it "runs strand jobs in strict order" do
      job1 = create_job(strand: "myjobs")
      job2 = create_job(strand: "myjobs")
      expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(job1)
      expect(Delayed::Job.get_and_lock_next_available("w2")).to be_nil
      job1.destroy
      # update time since the failed lock pushed it forward
      job2.run_at = 1.minute.ago
      job2.save!
      expect(Delayed::Job.get_and_lock_next_available("w3")).to eq(job2)
      expect(Delayed::Job.get_and_lock_next_available("w4")).to be_nil
    end

    it "fails to lock if an earlier job gets locked" do
      job1 = create_job(strand: "myjobs")
      job2 = create_job(strand: "myjobs")
      expect(Delayed::Job.find_available(2)).to eq([job1])
      expect(Delayed::Job.find_available(2)).to eq([job1])

      # job1 gets locked by w1
      expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(job1)

      # normally w2 would now be able to lock job2, but strands prevent it
      expect(Delayed::Job.get_and_lock_next_available("w2")).to be_nil

      # now job1 is done
      job1.destroy
      # update time since the failed lock pushed it forward
      job2.run_at = 1.minute.ago
      job2.save!
      expect(Delayed::Job.get_and_lock_next_available("w2")).to eq(job2)
    end

    it "keeps strand jobs in order as they are rescheduled" do
      job1 = create_job(strand: "myjobs")
      job2 = create_job(strand: "myjobs")
      job3 = create_job(strand: "myjobs")
      expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(job1)
      expect(Delayed::Job.find_available(1)).to eq([])
      job1.destroy
      expect(Delayed::Job.find_available(1)).to eq([job2])
      # move job2's time forward
      job2.run_at = 1.second.ago
      job2.save!
      job3.run_at = 5.seconds.ago
      job3.save!
      # we should still get job2, not job3
      expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(job2)
    end

    it "allows to run the next job if a failed job is present" do
      job1 = create_job(strand: "myjobs")
      job2 = create_job(strand: "myjobs")
      job1.fail!
      expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(job2)
    end

    it "does not interfere with jobs with no strand" do
      jobs = [create_job(strand: nil), create_job(strand: "myjobs")]
      locked = [Delayed::Job.get_and_lock_next_available("w1"),
                Delayed::Job.get_and_lock_next_available("w2")]
      expect(jobs).to eq locked
      expect(Delayed::Job.get_and_lock_next_available("w3")).to be_nil
    end

    it "does not interfere with jobs in other strands" do
      jobs = [create_job(strand: "strand1"), create_job(strand: "strand2")]
      locked = [Delayed::Job.get_and_lock_next_available("w1"),
                Delayed::Job.get_and_lock_next_available("w2")]
      expect(jobs).to eq locked
      expect(Delayed::Job.get_and_lock_next_available("w3")).to be_nil
    end

    it "does not find next jobs when given no priority" do
      jobs = [create_job(strand: "strand1"), create_job(strand: "strand1")]
      first = Delayed::Job.get_and_lock_next_available("w1", Delayed::Settings.queue, nil, nil)
      second = Delayed::Job.get_and_lock_next_available("w2", Delayed::Settings.queue, nil, nil)
      expect(first).to eq jobs.first
      expect(second).to be_nil
    end

    it "complains if you pass more than one strand-based option" do
      expect { create_job(strand: "a", n_strand: "b") }.to raise_error(ArgumentError)
    end

    context "singleton" do
      it "creates if there's no jobs on the strand" do
        @job = create_job(singleton: "myjobs")
        expect(@job).to be_present
        expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(@job)
      end

      it "creates if there's another job on the strand, but it's running" do
        @job = create_job(singleton: "myjobs")
        expect(@job).to be_present
        expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(@job)

        @job2 = create_job(singleton: "myjobs")
        expect(@job).to be_present
        expect(@job2).not_to eq(@job)
      end

      it "does not create if there's another non-running job on the strand" do
        @job = create_job(singleton: "myjobs")
        expect(@job).to be_present

        @job2 = create_job(singleton: "myjobs")
        expect(@job2).to be_new_record
      end

      it "does not create if there's a job running and one waiting on the strand" do
        @job = create_job(singleton: "myjobs")
        expect(@job).to be_present
        expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(@job)

        @job2 = create_job(singleton: "myjobs")
        expect(@job2).to be_present
        expect(@job2).not_to eq(@job)

        @job3 = create_job(singleton: "myjobs")
        expect(@job3).to be_new_record
      end

      it "updates existing job if new job is set to run sooner" do
        job1 = create_job(singleton: "myjobs", run_at: 1.hour.from_now)
        job2 = create_job(singleton: "myjobs")
        expect(job2).to eq(job1)
        # it should be scheduled to run immediately
        expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(job1)
      end

      it "updates existing job to a later date if requested" do
        t1 = 1.hour.from_now
        t2 = 2.hours.from_now
        job1 = create_job(singleton: "myjobs", run_at: t1)
        job2 = create_job(singleton: "myjobs", run_at: t2)
        expect(job2).to be_new_record

        job3 = create_job(singleton: "myjobs", run_at: t2, on_conflict: :overwrite)
        expect(job3).to eq(job1)
        expect(job3.run_at.to_i).to eq(t2.to_i)
      end

      it "updates existing singleton job handler if requested" do
        job1 = Delayed::Job.enqueue(SimpleJob.new, queue: nil, singleton: "myjobs", on_conflict: :overwrite)
        job2 = Delayed::Job.enqueue(ErrorJob.new, queue: nil, singleton: "myjobs", on_conflict: :overwrite)
        expect(job2).to eq(job1)
        expect(job1.reload.handler).to include("ErrorJob")
      end

      context "enqueue_result" do
        it "is :inserted if there's no jobs on the strand" do
          create_job(singleton: "different_singleton")
          job = create_job(singleton: "myjobs", on_conflict: :overwrite)
          expect(job.enqueue_result).to be :inserted
        end

        it "is :updated if there's already a singleton and on_conflict: :overwrite" do
          create_job(singleton: "myjobs", run_at: 1.hour.from_now)
          job = create_job(singleton: "myjobs", run_at: 2.hours.from_now, on_conflict: :overwrite)
          expect(job.enqueue_result).to be :updated
        end

        it "is :updated if there's already a singleton and on_conflict: :use_earliest and the job is updated" do
          create_job(singleton: "myjobs", run_at: 2.hours.from_now)
          job = create_job(singleton: "myjobs", run_at: 1.hour.from_now, on_conflict: :use_earliest)
          expect(job.enqueue_result).to be :updated
        end

        it "is :dropped if there's already a singleton and on_conflict: :use_earliest and the job is not updated" do
          create_job(singleton: "myjobs", run_at: 1.hour.from_now)
          job = create_job(singleton: "myjobs", run_at: 2.hours.from_now, on_conflict: :use_earliest)
          expect(job.enqueue_result).to be :dropped
        end

        it "is :dropped if there's already a singleton and on_conflict: :loose" do
          create_job(singleton: "myjobs")
          job = create_job(singleton: "myjobs", on_conflict: :loose)
          expect(job.enqueue_result).to be :dropped
        end

        it "is :updated if a job with a singleton and strand is overwritten" do
          create_job(singleton: "myjobs", strand: "mystrand")
          job = create_job(singleton: "myjobs", strand: "mystrand", on_conflict: :overwrite)
          expect(job.enqueue_result).to be :updated
        end

        it "is :dropped if a job with a singleton and strand is not overwritten" do
          create_job(singleton: "myjobs", strand: "mystrand")
          job = create_job(singleton: "myjobs", strand: "mystrand", on_conflict: :loose)
          expect(job.enqueue_result).to be :dropped
        end

        it "is :inserted if there's no singleton" do
          create_job(strand: "mystrand")
          job = create_job(strand: "mystrand")
          expect(job.enqueue_result).to be :inserted
        end
      end

      context "next_in_strand management - deadlocks and race conditions", :non_transactional, :slow do
        # The following unit tests are fairly slow and non-deterministic. It may be
        # easier to make them fail quicker and more consistently by adding a random
        # sleep into the appropriate trigger(s).

        def loop_secs(val)
          loop_start = Time.now.utc

          loop do
            break if Time.now.utc >= loop_start + val

            yield
          end
        end

        def loop_until_found(params)
          found = false

          loop_secs(10.seconds) do
            if Delayed::Job.exists?(**params)
              found = true
              break
            end
          end

          raise "timed out waiting for condition" unless found
        end

        def thread_body
          yield
        rescue
          Thread.current.thread_variable_set(:fail, true)
          raise
        end

        it "doesn't orphan the singleton when two are queued consecutively" do
          # In order to reproduce this one efficiently, you'll probably want to add
          # a sleep within delayed_jobs_before_insert_row_tr_fn.
          # IF NEW.singleton IS NOT NULL THEN
          #   ...
          #   PERFORM pg_sleep(random() * 2);
          # END IF;

          threads = []

          threads << Thread.new do
            thread_body do
              loop do
                create_job(singleton: "singleton_job")
                create_job(singleton: "singleton_job")
              end
            end
          end

          threads << Thread.new do
            thread_body do
              loop do
                Delayed::Job.get_and_lock_next_available("w1")&.destroy
              end
            end
          end

          threads << Thread.new do
            thread_body do
              loop do
                loop_until_found(singleton: "singleton_job", next_in_strand: true)
              end
            end
          end

          begin
            loop_secs(60.seconds) do
              if threads.any? { |x| x.thread_variable_get(:fail) }
                raise "at least one job became orphaned or other error"
              end
            end
          ensure
            threads.each(&:kill)
            threads.each(&:join)
          end
        end

        it "doesn't deadlock when transitioning from strand_a to strand_b" do
          # In order to reproduce this one efficiently, you'll probably want to add
          # a sleep within delayed_jobs_after_delete_row_tr_fn.
          # PERFORM pg_advisory_xact_lock(half_md5_as_bigint(OLD.strand));
          # PERFORM pg_sleep(random() * 2);

          threads = []

          threads << Thread.new do
            thread_body do
              loop do
                j1 = create_job(singleton: "myjobs", strand: "myjobs2", locked_by: "w1")
                j2 = create_job(singleton: "myjobs", strand: "myjobs")

                j1.delete
                j2.delete
              end
            end
          end

          threads << Thread.new do
            thread_body do
              loop do
                j1 = create_job(singleton: "myjobs2", strand: "myjobs", locked_by: "w1")
                j2 = create_job(singleton: "myjobs2", strand: "myjobs2")

                j1.delete
                j2.delete
              end
            end
          end

          threads << Thread.new do
            thread_body do
              loop do
                loop_until_found(singleton: "myjobs", next_in_strand: true)
              end
            end
          end

          threads << Thread.new do
            thread_body do
              loop do
                loop_until_found(singleton: "myjobs2", next_in_strand: true)
              end
            end
          end

          begin
            loop_secs(60.seconds) do
              if threads.any? { |x| x.thread_variable_get(:fail) }
                raise "at least one thread hit a deadlock or other error"
              end
            end
          ensure
            threads.each(&:kill)
            threads.each(&:join)
          end
        end
      end

      context "next_in_strand management" do
        it "handles transitions correctly when going from stranded to not stranded" do
          @job1 = create_job(singleton: "myjobs", strand: "myjobs")
          Delayed::Job.get_and_lock_next_available("w1")
          @job2 = create_job(singleton: "myjobs")

          expect(@job1.reload.next_in_strand).to be true
          expect(@job2.reload.next_in_strand).to be false

          @job1.destroy
          expect(@job2.reload.next_in_strand).to be true
        end

        it "handles transitions correctly when going from not stranded to stranded" do
          @job1 = create_job(singleton: "myjobs2", strand: "myjobs")
          @job2 = create_job(singleton: "myjobs")
          Delayed::Job.get_and_lock_next_available("w1")
          Delayed::Job.get_and_lock_next_available("w1")
          @job3 = create_job(singleton: "myjobs", strand: "myjobs2")

          expect(@job1.reload.next_in_strand).to be true
          expect(@job2.reload.next_in_strand).to be true
          expect(@job3.reload.next_in_strand).to be false

          @job2.destroy
          expect(@job1.reload.next_in_strand).to be true
          expect(@job3.reload.next_in_strand).to be true
        end

        it "does not violate n_strand=1 constraints when going from not stranded to stranded" do
          @job1 = create_job(singleton: "myjobs2", strand: "myjobs")
          @job2 = create_job(singleton: "myjobs")
          Delayed::Job.get_and_lock_next_available("w1")
          Delayed::Job.get_and_lock_next_available("w1")
          @job3 = create_job(singleton: "myjobs", strand: "myjobs")

          expect(@job1.reload.next_in_strand).to be true
          expect(@job2.reload.next_in_strand).to be true
          expect(@job3.reload.next_in_strand).to be false

          @job2.destroy
          expect(@job1.reload.next_in_strand).to be true
          expect(@job3.reload.next_in_strand).to be false
        end

        it "handles transitions correctly when going from stranded to another strand" do
          @job1 = create_job(singleton: "myjobs", strand: "myjobs")
          Delayed::Job.get_and_lock_next_available("w1")
          @job2 = create_job(singleton: "myjobs", strand: "myjobs2")

          expect(@job1.reload.next_in_strand).to be true
          expect(@job2.reload.next_in_strand).to be false

          @job1.destroy
          expect(@job2.reload.next_in_strand).to be true
        end

        it "does not violate n_strand=1 constraints when going from stranded to another strand" do
          @job1 = create_job(singleton: "myjobs2", strand: "myjobs2")
          @job2 = create_job(singleton: "myjobs", strand: "myjobs")
          Delayed::Job.get_and_lock_next_available("w1")
          Delayed::Job.get_and_lock_next_available("w1")
          @job3 = create_job(singleton: "myjobs", strand: "myjobs2")

          expect(@job1.reload.next_in_strand).to be true
          expect(@job2.reload.next_in_strand).to be true
          expect(@job3.reload.next_in_strand).to be false

          @job2.destroy
          expect(@job1.reload.next_in_strand).to be true
          expect(@job3.reload.next_in_strand).to be false
        end

        it "creates first as true, and second as false, then transitions to second when deleted" do
          @job1 = create_job(singleton: "myjobs")
          Delayed::Job.get_and_lock_next_available("w1")
          @job2 = create_job(singleton: "myjobs")
          expect(@job1.reload.next_in_strand).to be true
          expect(@job2.reload.next_in_strand).to be false

          @job1.destroy
          expect(@job2.reload.next_in_strand).to be true
        end

        it "when combined with a strand" do
          job1 = create_job(singleton: "singleton", strand: "strand")
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job1
          job2 = create_job(singleton: "singleton", strand: "strand")
          expect(job2).not_to eq job1
          expect(job2).not_to be_new_record
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job3 = create_job(strand: "strand")
          job4 = create_job(strand: "strand")
          expect(job3.reload).not_to be_next_in_strand
          expect(job4.reload).not_to be_next_in_strand
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job1.destroy
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job2
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job2.destroy
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job3
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job3.destroy
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job4
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
        end

        it "when combined with a small n_strand" do
          allow(Delayed::Settings).to receive(:num_strands).and_return(->(*) { 2 })

          job1 = create_job(singleton: "singleton", n_strand: "strand")
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job1
          job2 = create_job(singleton: "singleton", n_strand: "strand")
          expect(job2).not_to eq job1
          expect(job2).not_to be_new_record
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job3 = create_job(n_strand: "strand")
          job4 = create_job(n_strand: "strand")
          expect(job3.reload).to be_next_in_strand
          expect(job4.reload).not_to be_next_in_strand
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job3
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          # this doesn't unlock job2, even though it's ahead of job4
          job3.destroy
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job4
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job4.destroy
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job1.destroy
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job2
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
        end

        it "when combined with a larger n_strand" do
          allow(Delayed::Settings).to receive(:num_strands).and_return(->(*) { 10 })

          job1 = create_job(singleton: "singleton", n_strand: "strand")
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job1
          job2 = create_job(singleton: "singleton", n_strand: "strand")
          expect(job2).not_to eq job1
          expect(job2).not_to be_new_record
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job3 = create_job(n_strand: "strand")
          job4 = create_job(n_strand: "strand")
          expect(job3.reload).to be_next_in_strand
          expect(job4.reload).to be_next_in_strand
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job3
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job4
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          # this doesn't unlock job2
          job3.destroy
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job4.destroy
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
          job1.destroy
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq job2
          expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil
        end
      end

      context "with on_conflict: loose and strand-inferred-from-singleton" do
        around do |example|
          Delayed::Settings.infer_strand_from_singleton = true
          example.call
        ensure
          Delayed::Settings.infer_strand_from_singleton = false
        end

        it "does not create if there's another non-running job on the strand" do
          @job = create_job(singleton: "myjobs", on_conflict: :loose)
          expect(@job).to be_present

          @job2 = create_job(singleton: "myjobs", on_conflict: :loose)
          expect(@job2).to be_new_record
        end
      end

      context "when unlocking with another singleton pending" do
        it "deletes the pending singleton" do
          @job1 = create_job(singleton: "myjobs", max_attempts: 2)
          expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(@job1)

          @job2 = create_job(singleton: "myjobs", max_attempts: 2)

          @job1.reload.reschedule
          expect { @job1.reload }.not_to raise_error
          expect { @job2.reload }.to raise_error(ActiveRecord::RecordNotFound)
        end
      end
    end
  end

  context "on hold" do
    it "hold/unholds jobs" do
      job1 = create_job
      job1.hold!
      expect(Delayed::Job.get_and_lock_next_available("w1")).to be_nil

      job1.unhold!
      expect(Delayed::Job.get_and_lock_next_available("w1")).to eq(job1)
    end
  end

  context "periodic jobs" do
    before do
      # make the periodic job get scheduled in the past
      @cron_time = 10.minutes.ago
      allow(Delayed::Periodic).to receive(:now).and_return(@cron_time)
      Delayed::Periodic.scheduled = {}
      Delayed::Periodic.cron("my SimpleJob", "*/5 * * * * *") do
        Delayed::Job.enqueue(SimpleJob.new)
      end
    end

    it "schedules jobs if they aren't scheduled yet" do
      expect(Delayed::Job.jobs_count(:current)).to eq(0)
      Delayed::Periodic.perform_audit!
      expect(Delayed::Job.jobs_count(:current)).to eq(1)
      job = Delayed::Job.get_and_lock_next_available("test1")
      expect(job.tag).to eq("periodic: my SimpleJob")
      expect(job.payload_object).to eq(Delayed::Periodic.scheduled["my SimpleJob"])
      expect(job.run_at).to be >= @cron_time
      expect(job.run_at).to be <= @cron_time + 6.minutes
      expect(job.singleton).to eq(job.tag)
    end

    it "schedules jobs if there are only failed jobs on the queue" do
      expect(Delayed::Job.jobs_count(:current)).to eq(0)
      expect { Delayed::Periodic.perform_audit! }.to change { Delayed::Job.jobs_count(:current) }.by(1)
      expect(Delayed::Job.jobs_count(:current)).to eq(1)
      job = Delayed::Job.get_and_lock_next_available("test1")
      job.fail!
      expect { Delayed::Periodic.perform_audit! }.to change { Delayed::Job.jobs_count(:current) }.by(1)
    end

    it "does not schedule jobs that are already scheduled" do
      expect(Delayed::Job.jobs_count(:current)).to eq(0)
      Delayed::Periodic.perform_audit!
      expect(Delayed::Job.jobs_count(:current)).to eq(1)
      job = Delayed::Job.find_available(1).first
      Delayed::Periodic.perform_audit!
      expect(Delayed::Job.jobs_count(:current)).to eq(1)
      # verify that the same job still exists, it wasn't just replaced with a new one
      expect(job).to eq(Delayed::Job.find_available(1).first)
    end

    it "schedules the next job run after performing" do
      expect(Delayed::Job.jobs_count(:current)).to eq(0)
      Delayed::Periodic.perform_audit!
      expect(Delayed::Job.jobs_count(:current)).to eq(1)
      job = Delayed::Job.get_and_lock_next_available("test")
      run_job(job)

      job = Delayed::Job.get_and_lock_next_available("test1")
      expect(job.tag).to eq("SimpleJob#perform")

      next_scheduled = Delayed::Job.get_and_lock_next_available("test2")
      expect(next_scheduled.tag).to eq("periodic: my SimpleJob")
      expect(next_scheduled.payload_object).to be_is_a(Delayed::Periodic)
    end

    it "rejects duplicate named jobs" do
      expect { Delayed::Periodic.cron("my SimpleJob", "*/15 * * * * *") { nil } }.to raise_error(ArgumentError)
    end

    it "handles jobs that are no longer scheduled" do
      Delayed::Periodic.perform_audit!
      Delayed::Periodic.scheduled = {}
      job = Delayed::Job.get_and_lock_next_available("test")
      run_job(job)
      # shouldn't error, and the job should now be deleted
      expect(Delayed::Job.jobs_count(:current)).to eq(0)
    end

    it "allows overriding schedules using periodic_jobs.yml" do
      change_setting(Delayed::Periodic, :overrides, { "my ChangedJob" => "*/10 * * * * *" }) do
        Delayed::Periodic.scheduled = {}
        Delayed::Periodic.cron("my ChangedJob", "*/5 * * * * *") do
          Delayed::Job.enqueue(SimpleJob.new)
        end
        expect(Delayed::Periodic.scheduled["my ChangedJob"].cron.original).to eq("*/10 * * * * *")
      end
    end

    it "fails if the override cron line is invalid" do
      change_setting(Delayed::Periodic, :overrides, { "my ChangedJob" => "*/10 * * * * * *" }) do # extra asterisk
        Delayed::Periodic.scheduled = {}
        expect do
          Delayed::Periodic.cron("my ChangedJob", "*/5 * * * * *") do
            Delayed::Job.enqueue(SimpleJob.new)
          end
        end.to raise_error(ArgumentError)
      end

      expect do
        Delayed::Periodic.add_overrides({ "my ChangedJob" => "*/10 * * * * * *" })
      end.to raise_error(ArgumentError)
    end
  end

  it "sets in_delayed_job?" do
    job = InDelayedJobTest.delay(ignore_transaction: true).check_in_job
    expect(Delayed::Job.in_delayed_job?).to be(false)
    job.invoke_job
    expect(Delayed::Job.in_delayed_job?).to be(false)
  end

  it "fails on job creation if an unsaved AR object is used" do
    story = Story.new text: "Once upon..."
    expect { story.delay.text }.to raise_error(RuntimeError)

    reader = StoryReader.new
    expect { reader.delay.read(story) }.to raise_error(RuntimeError)

    expect { [story, 1, story, false].delay.first }.to raise_error(RuntimeError)
  end

  # the sort order of current_jobs and list_jobs depends on the back-end
  # implementation, so sort order isn't tested in these specs
  describe "current jobs, queue size, strand_size" do
    before do
      @jobs = []
      3.times { @jobs << create_job(priority: 3) }
      @jobs.unshift create_job(priority: 2)
      @jobs.unshift create_job(priority: 1)
      @jobs << create_job(priority: 3, strand: "test1")
      @future_job = create_job(run_at: 5.hours.from_now)
      2.times { @jobs << create_job(priority: 3) }
      @jobs << create_job(priority: 3, strand: "test1")
      @failed_job = create_job.tap(&:fail!)
      @other_queue_job = create_job(queue: "another")
    end

    it "returns the queued jobs" do
      expect(Delayed::Job.list_jobs(:current, 100).map(&:id).sort).to eq(@jobs.map(&:id).sort)
    end

    it "paginates the returned jobs" do
      @returned = []
      @returned += Delayed::Job.list_jobs(:current, 3, 0)
      @returned += Delayed::Job.list_jobs(:current, 4, 3)
      @returned += Delayed::Job.list_jobs(:current, 100, 7)
      expect(@returned.sort_by(&:id)).to eq(@jobs.sort_by(&:id))
    end

    it "returns other queues" do
      expect(Delayed::Job.list_jobs(:current, 5, 0, "another")).to eq([@other_queue_job])
    end

    it "returns queue size" do
      expect(Delayed::Job.jobs_count(:current)).to eq(@jobs.size)
      expect(Delayed::Job.jobs_count(:current, "another")).to eq(1)
      expect(Delayed::Job.jobs_count(:current, "bogus")).to eq(0)
    end

    it "returns strand size" do
      expect(Delayed::Job.strand_size("test1")).to eq(2)
      expect(Delayed::Job.strand_size("bogus")).to eq(0)
    end
  end

  it "returns the jobs in a strand" do
    strand_jobs = []
    3.times { strand_jobs << create_job(strand: "test1") }
    2.times { create_job(strand: "test2") }
    strand_jobs << create_job(strand: "test1", run_at: 2.seconds.from_now)
    create_job

    jobs = Delayed::Job.list_jobs(:strand, 3, 0, "test1")
    expect(jobs.size).to eq(3)

    jobs += Delayed::Job.list_jobs(:strand, 3, 3, "test1")
    expect(jobs.size).to eq(4)

    expect(jobs.sort_by(&:id)).to eq(strand_jobs.sort_by(&:id))
  end

  it "returns the jobs for a tag" do
    tag_jobs = []
    3.times { tag_jobs << "test".delay(ignore_transaction: true).to_s }
    2.times { "test".delay.to_i }
    tag_jobs << "test".delay(ignore_transaction: true, run_at: 5.hours.from_now).to_s
    tag_jobs << "test".delay(ignore_transaction: true, strand: "test1").to_s
    "test".delay(strand: "test1").to_i
    create_job

    jobs = Delayed::Job.list_jobs(:tag, 3, 0, "String#to_s")
    expect(jobs.size).to eq(3)

    jobs += Delayed::Job.list_jobs(:tag, 3, 3, "String#to_s")
    expect(jobs.size).to eq(5)

    expect(jobs.sort_by(&:id)).to eq(tag_jobs.sort_by(&:id))
  end

  describe "running_jobs" do
    it "returns the running jobs, ordered by locked_at" do
      Timecop.freeze(10.minutes.ago) { 3.times { create_job } }
      j1 = Timecop.freeze(2.minutes.ago) { Delayed::Job.get_and_lock_next_available("w1") }
      j2 = Timecop.freeze(5.minutes.ago) { Delayed::Job.get_and_lock_next_available("w2") }
      j3 = Timecop.freeze(5.seconds.ago) { Delayed::Job.get_and_lock_next_available("w3") }
      expect([j1, j2, j3].compact.size).to eq(3)

      expect(Delayed::Job.running_jobs).to eq([j2, j1, j3])
    end
  end

  describe "future jobs" do
    it "finds future jobs once their run_at rolls by" do
      Timecop.freeze do
        @job = create_job run_at: 5.minutes.from_now
        expect(Delayed::Job.find_available(5)).not_to include(@job)
      end
      Timecop.freeze(1.hour.from_now) do
        expect(Delayed::Job.find_available(5)).to include(@job)
        expect(Delayed::Job.get_and_lock_next_available("test")).to eq(@job)
      end
    end

    it "returns future jobs sorted by their run_at" do
      @j1 = create_job
      @j2 = create_job run_at: 1.hour.from_now
      @j3 = create_job run_at: 30.minutes.from_now
      expect(Delayed::Job.list_jobs(:future, 1)).to eq([@j3])
      expect(Delayed::Job.list_jobs(:future, 5)).to eq([@j3, @j2])
      expect(Delayed::Job.list_jobs(:future, 1, 1)).to eq([@j2])
    end
  end

  describe "failed jobs" do
    # the sort order of failed_jobs depends on the back-end implementation,
    # so sort order isn't tested here
    it "returns the list of failed jobs" do
      jobs = []
      3.times { jobs << create_job(priority: 3) }
      jobs = jobs.sort_by(&:id)
      expect(Delayed::Job.list_jobs(:failed, 1)).to eq([])
      jobs[0].fail!
      jobs[1].fail!
      failed = (Delayed::Job.list_jobs(:failed, 1, 0) + Delayed::Job.list_jobs(:failed, 1, 1)).sort_by(&:id)
      expect(failed.size).to eq(2)
      expect(failed[0].original_job_id).to eq(jobs[0].id)
      expect(failed[1].original_job_id).to eq(jobs[1].id)
    end
  end

  describe "bulk_update" do
    shared_examples_for "scope" do
      before do
        @affected_jobs = []
        @ignored_jobs = []
      end

      it "holds and unhold a scope of jobs" do
        expect(@affected_jobs.all?(&:on_hold?)).to be false
        expect(@ignored_jobs.any?(&:on_hold?)).to be false
        expect(Delayed::Job.bulk_update("hold", flavor: @flavor, query: @query)).to eq(@affected_jobs.size)

        expect(@affected_jobs.all? { |j| Delayed::Job.find(j.id).on_hold? }).to be true
        expect(@ignored_jobs.any? { |j| Delayed::Job.find(j.id).on_hold? }).to be false

        expect(Delayed::Job.bulk_update("unhold", flavor: @flavor, query: @query)).to eq(@affected_jobs.size)

        expect(@affected_jobs.any? { |j| Delayed::Job.find(j.id).on_hold? }).to be false
        expect(@ignored_jobs.any? { |j| Delayed::Job.find(j.id).on_hold? }).to be false
      end

      it "deletes a scope of jobs" do
        expect(Delayed::Job.bulk_update("destroy", flavor: @flavor, query: @query)).to eq(@affected_jobs.size)
        expect(Delayed::Job.where(id: @affected_jobs.map(&:id))).not_to exist
        expect(Delayed::Job.where(id: @ignored_jobs.map(&:id)).count).to eq @ignored_jobs.size
      end
    end

    describe "scope: current" do
      include_examples "scope"
      before do # rubocop:disable RSpec/HooksBeforeExamples
        @flavor = "current"
        Timecop.freeze(5.minutes.ago) do
          3.times { @affected_jobs << create_job }
          @ignored_jobs << create_job(run_at: 2.hours.from_now)
          @ignored_jobs << create_job(queue: "q2")
        end
      end
    end

    describe "scope: future" do
      include_examples "scope"
      before do # rubocop:disable RSpec/HooksBeforeExamples
        @flavor = "future"
        Timecop.freeze(5.minutes.ago) do
          3.times { @affected_jobs << create_job(run_at: 2.hours.from_now) }
          @ignored_jobs << create_job
          @ignored_jobs << create_job(queue: "q2", run_at: 2.hours.from_now)
        end
      end
    end

    describe "scope: strand" do
      include_examples "scope"
      before do # rubocop:disable RSpec/HooksBeforeExamples
        @flavor = "strand"
        @query = "s1"
        Timecop.freeze(5.minutes.ago) do
          @affected_jobs << create_job(strand: "s1")
          @affected_jobs << create_job(strand: "s1", run_at: 3.seconds.from_now)
          @ignored_jobs << create_job
          @ignored_jobs << create_job(strand: "s2")
          @ignored_jobs << create_job(strand: "s2", run_at: 3.seconds.from_now)
        end
      end
    end

    describe "scope: tag" do
      include_examples "scope"
      before do # rubocop:disable RSpec/HooksBeforeExamples
        @flavor = "tag"
        @query = "String#to_i"
        Timecop.freeze(5.minutes.ago) do
          @affected_jobs << "test".delay(ignore_transaction: true).to_i
          @affected_jobs << "test".delay(strand: "s1", ignore_transaction: true).to_i
          @affected_jobs << "test".delay(run_at: 2.hours.from_now, ignore_transaction: true).to_i
          @ignored_jobs << create_job
          @ignored_jobs << create_job(run_at: 1.hour.from_now)
        end
      end
    end

    it "holds and un-hold given job ids" do
      j1 = "test".delay(ignore_transaction: true).to_i
      j2 = create_job(run_at: 2.hours.from_now)
      j3 = "test".delay(strand: "s1", ignore_transaction: true).to_i
      expect(Delayed::Job.bulk_update("hold", ids: [j1.id, j2.id])).to eq(2)
      expect(Delayed::Job.find(j1.id).on_hold?).to be true
      expect(Delayed::Job.find(j2.id).on_hold?).to be true
      expect(Delayed::Job.find(j3.id).on_hold?).to be false

      expect(Delayed::Job.bulk_update("unhold", ids: [j2.id, j3.id])).to eq(1)
      expect(Delayed::Job.find(j1.id).on_hold?).to be true
      expect(Delayed::Job.find(j2.id).on_hold?).to be false
      expect(Delayed::Job.find(j3.id).on_hold?).to be false
    end

    it "does not hold locked jobs" do
      job1 = Delayed::Job.new(tag: "tag")
      job1.create_and_lock!("worker")
      expect(job1.on_hold?).to be false
      expect(Delayed::Job.bulk_update("hold", ids: [job1.id])).to eq(0)
      expect(Delayed::Job.find(job1.id).on_hold?).to be false
    end

    it "does not unhold locked jobs" do
      job1 = Delayed::Job.new(tag: "tag")
      job1.create_and_lock!("worker")
      expect(Delayed::Job.bulk_update("unhold", ids: [job1.id])).to eq(0)
      expect(Delayed::Job.find(job1.id).on_hold?).to be false
      expect(Delayed::Job.find(job1.id).locked?).to be true
    end

    it "deletes given job ids" do
      jobs = (0..2).map { create_job }
      expect(Delayed::Job.bulk_update("destroy", ids: jobs[0, 2].map(&:id))).to eq(2)
      expect(Delayed::Job.order(:id).where(id: jobs.map(&:id))).to eq jobs[2, 1]
    end

    it "does not delete locked jobs" do
      job1 = Delayed::Job.new(tag: "tag")
      job1.create_and_lock!("worker")
      expect(Delayed::Job.bulk_update("destroy", ids: [job1.id])).to eq(0)
      expect(Delayed::Job.find(job1.id).locked?).to be true
    end
  end

  describe "tag_counts" do
    before do
      @cur = []
      3.times { @cur << "test".delay(ignore_transaction: true).to_s }
      5.times { @cur << "test".delay(ignore_transaction: true).to_i }
      2.times { @cur << "test".delay(ignore_transaction: true).upcase }
      "test".delay(ignore_transaction: true).downcase.fail!
      @future = []
      5.times { @future << "test".delay(run_at: 3.hours.from_now, ignore_transaction: true).downcase }
      @cur << "test".delay(ignore_transaction: true).downcase
    end

    it "returns a sorted list of popular current tags" do
      expect(Delayed::Job.tag_counts(:current, 1)).to eq([{ tag: "String#to_i", count: 5 }])
      expect(Delayed::Job.tag_counts(:current, 1, 1)).to eq([{ tag: "String#to_s", count: 3 }])
      expect(Delayed::Job.tag_counts(:current, 5)).to eq([{ tag: "String#to_i", count: 5 },
                                                          { tag: "String#to_s", count: 3 },
                                                          { tag: "String#upcase", count: 2 },
                                                          { tag: "String#downcase", count: 1 }])
      @cur[0, 4].each(&:destroy)
      @future[0].run_at = @future[1].run_at = 1.hour.ago
      @future[0].save!
      @future[1].save!

      expect(Delayed::Job.tag_counts(:current, 5)).to eq([{ tag: "String#to_i", count: 4 },
                                                          { tag: "String#downcase", count: 3 },
                                                          { tag: "String#upcase", count: 2 }])
    end

    it "returns a sorted list of all popular tags" do
      expect(Delayed::Job.tag_counts(:all, 1)).to eq([{ tag: "String#downcase", count: 6 }])
      expect(Delayed::Job.tag_counts(:all, 1, 1)).to eq([{ tag: "String#to_i", count: 5 }])
      expect(Delayed::Job.tag_counts(:all, 5)).to eq([{ tag: "String#downcase", count: 6 },
                                                      { tag: "String#to_i", count: 5 },
                                                      { tag: "String#to_s", count: 3 },
                                                      { tag: "String#upcase", count: 2 }])

      @cur[0, 4].each(&:destroy)
      @future[0].destroy
      @future[1].fail!
      @future[2].fail!

      expect(Delayed::Job.tag_counts(:all, 5)).to eq([{ tag: "String#to_i", count: 4 },
                                                      { tag: "String#downcase", count: 3 },
                                                      { tag: "String#upcase", count: 2 }])
    end
  end

  it "unlocks orphaned jobs" do
    change_setting(Delayed::Settings, :max_attempts, 2) do
      job1 = Delayed::Job.new(tag: "tag")
      job2 = Delayed::Job.new(tag: "tag")
      job3 = Delayed::Job.new(tag: "tag")
      job4 = Delayed::Job.new(tag: "tag")
      job1.create_and_lock!("Jobworker:#{Process.pid}")
      `echo ''`
      child_pid = $?.pid
      job2.create_and_lock!("Jobworker:#{child_pid}")
      job3.create_and_lock!("someoneelse:#{Process.pid}")
      job4.create_and_lock!("Jobworker:notanumber")

      expect(Delayed::Job.unlock_orphaned_jobs(nil, "Jobworker")).to eq(1)

      expect(Delayed::Job.find(job1.id).locked_by).not_to be_nil
      expect(Delayed::Job.find(job2.id).locked_by).to be_nil
      expect(Delayed::Job.find(job3.id).locked_by).not_to be_nil
      expect(Delayed::Job.find(job4.id).locked_by).not_to be_nil

      expect(Delayed::Job.unlock_orphaned_jobs(nil, "Jobworker")).to eq(0)
    end
  end

  it "removes an un-reschedulable job" do
    change_setting(Delayed::Settings, :max_attempts, -1) do
      job = Delayed::Job.new(tag: "tag")
      `echo ''`
      child_pid = $?.pid
      job.create_and_lock!("Jobworker:#{child_pid}")
      Timeout.timeout(1) do
        # if this takes longer than a second it's hung
        # in an infinite loop, which would be bad.
        expect(Delayed::Job.unlock_orphaned_jobs(nil, "Jobworker")).to eq(1)
      end
      expect { Delayed::Job.find(job.id) }.to raise_error(ActiveRecord::RecordNotFound)
    end
  end

  it "unlocks orphaned jobs given a pid" do
    change_setting(Delayed::Settings, :max_attempts, 2) do
      job1 = Delayed::Job.new(tag: "tag")
      job2 = Delayed::Job.new(tag: "tag")
      job3 = Delayed::Job.new(tag: "tag")
      job4 = Delayed::Job.new(tag: "tag")
      job1.create_and_lock!("Jobworker:#{Process.pid}")
      `echo ''`
      child_pid = $?.pid
      `echo ''`
      child_pid2 = $?.pid
      job2.create_and_lock!("Jobworker:#{child_pid}")
      job3.create_and_lock!("someoneelse:#{Process.pid}")
      job4.create_and_lock!("Jobworker:notanumber")

      expect(Delayed::Job.unlock_orphaned_jobs(child_pid2, "Jobworker")).to eq(0)
      expect(Delayed::Job.unlock_orphaned_jobs(child_pid, "Jobworker")).to eq(1)

      expect(Delayed::Job.find(job1.id).locked_by).not_to be_nil
      expect(Delayed::Job.find(job2.id).locked_by).to be_nil
      expect(Delayed::Job.find(job3.id).locked_by).not_to be_nil
      expect(Delayed::Job.find(job4.id).locked_by).not_to be_nil

      expect(Delayed::Job.unlock_orphaned_jobs(child_pid, "Jobworker")).to eq(0)
    end
  end
end
