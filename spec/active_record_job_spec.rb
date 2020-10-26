# frozen_string_literal: true

require File.expand_path("../spec_helper", __FILE__)

describe 'Delayed::Backed::ActiveRecord::Job' do
  before :all do
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
  end

  after :all do
    Delayed.send(:remove_const, :Job)
  end

  before do
    Delayed::Testing.clear_all!
  end

  include_examples 'a delayed_jobs implementation'

  it "should recover as well as possible from a failure failing a job" do
    allow(Delayed::Job::Failed).to receive(:create).and_raise(RuntimeError)
    job = "test".delay(ignore_transaction: true).reverse
    job_id = job.id
    proc { job.fail! }.should raise_error(RuntimeError)
    proc { Delayed::Job.find(job_id) }.should raise_error(ActiveRecord::RecordNotFound)
    Delayed::Job.count.should == 0
  end

  context "when another worker has worked on a task since the job was found to be available, it" do
    before :each do
      @job = Delayed::Job.create :payload_object => SimpleJob.new
      @job_copy_for_worker_2 = Delayed::Job.find(@job.id)
    end

    it "should not allow a second worker to get exclusive access if already successfully processed by worker1" do
      @job.destroy
      @job_copy_for_worker_2.send(:lock_exclusively!, 'worker2').should == false
    end

    it "should not allow a second worker to get exclusive access if failed to be processed by worker1 and run_at time is now in future (due to backing off behaviour)" do
      @job.update(:attempts => 1, :run_at => 1.day.from_now)
      @job_copy_for_worker_2.send(:lock_exclusively!, 'worker2').should == false
    end

    it "should select the next job at random if enabled" do
      begin
        Delayed::Settings.select_random_from_batch = true
        15.times { "test".delay.length }
        founds = []
        15.times do
          job = Delayed::Job.get_and_lock_next_available('tester')
          founds << job
          job.unlock
          job.save!
        end
        founds.uniq.size.should > 1
      ensure
        Delayed::Settings.select_random_from_batch = false
      end
    end
  end

  it "should unlock a successfully locked job and persist the job's unlocked state" do
    job = Delayed::Job.create :payload_object => SimpleJob.new
    job.send(:lock_exclusively!, 'worker1').should == true
    job.reload
    job.unlock
    job.save!
    job.reload
    job.locked_by.should == nil
    job.locked_at.should == nil
  end

  describe "bulk_update failed jobs" do
    context "holding/unholding failed jobs" do
      before :each do
        @job = Delayed::Job.create :payload_object => SimpleJob.new
        Delayed::Job.get_and_lock_next_available('worker1').should == @job
        @job.fail!
      end

      it "should raise error when holding failed jobs" do
        expect { Delayed::Job.bulk_update('hold', :flavor => 'failed', :query => @query) }.to raise_error(RuntimeError)
      end

      it "should raise error unholding failed jobs" do
        expect { Delayed::Job.bulk_update('unhold', :flavor => 'failed', :query => @query) }.to raise_error(RuntimeError)
      end
    end

    context "deleting failed jobs" do
      before :each do
        2.times {
          j = Delayed::Job.create(:payload_object => SimpleJob.new)
          j.send(:lock_exclusively!, 'worker1').should == true
          j.fail!
        }
      end

      it "should delete failed jobs by id" do
        target_ids = Delayed::Job::Failed.all[0..2].map { |j| j.id }
        Delayed::Job.bulk_update('destroy', :ids => target_ids, :flavor => 'failed', :query => @query).should == target_ids.length
      end

      it "should delete all failed jobs" do
        failed_count = Delayed::Job::Failed.count
        Delayed::Job.bulk_update('destroy', :flavor => 'failed', :query => @query).should == failed_count
      end
    end
  end

  context 'n_strand' do
    it "should default to 1" do
      expect(Delayed::Job).to receive(:rand).never
      job = Delayed::Job.enqueue(SimpleJob.new, :n_strand => 'njobs')
      job.strand.should == "njobs"
    end

    it "should set max_concurrent based on num_strands" do
      change_setting(Delayed::Settings, :num_strands, ->(strand_name) { expect(strand_name).to eql "njobs"; "3" }) do
        job = Delayed::Job.enqueue(SimpleJob.new, :n_strand => 'njobs')
        job.strand.should == "njobs"
        job.max_concurrent.should == 3
      end
    end

    context "with two parameters" do
      it "should use the first param as the setting to read" do
        job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "123"])
        job.strand.should == "njobs/123"
        change_setting(Delayed::Settings, :num_strands, ->(strand_name) {
          case strand_name
          when "njobs"; 3
          else nil
          end
        }) do
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "123"])
          job.strand.should == "njobs/123"
          job.max_concurrent.should == 3
        end
      end

      it "should allow overridding the setting based on the second param" do
        change_setting(Delayed::Settings, :num_strands, ->(strand_name) {
          case strand_name
          when "njobs/123"; 5
          else nil
          end
        }) do
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "123"])
          job.strand.should == "njobs/123"
          job.max_concurrent.should == 5
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "456"])
          job.strand.should == "njobs/456"
          job.max_concurrent.should == 1
        end

        change_setting(Delayed::Settings, :num_strands, ->(strand_name) {
          case strand_name
          when "njobs/123"; 5
          when "njobs"; 3
          else nil
          end
        }) do
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "123"])
          job.strand.should == "njobs/123"
          job.max_concurrent.should == 5
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "456"])
          job.strand.should == "njobs/456"
          job.max_concurrent.should == 3
        end
      end
    end

    context "max_concurrent triggers" do
      before do
        skip("postgres specific") unless ActiveRecord::Base.connection.adapter_name == 'PostgreSQL'
      end

      it "should set one job as next_in_strand at a time with max_concurrent of 1" do
        job1 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
        job1.reload
        job1.next_in_strand.should == true
        job2 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
        job2.reload
        job2.next_in_strand.should == false
        run_job(job1)
        job2.reload
        job2.next_in_strand.should == true
      end

      it "should set multiple jobs as next_in_strand at a time based on max_concurrent" do
        change_setting(Delayed::Settings, :num_strands, ->(strand_name) {
          case strand_name
          when "njobs"; 2
          else nil
          end
        }) do
          job1 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
          job1.reload
          job1.next_in_strand.should == true
          job2 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
          job2.reload
          job2.next_in_strand.should == true
          job3 = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs"])
          job3.reload
          job3.next_in_strand.should == false
          run_job(job1)
          job3.reload
          job3.next_in_strand.should == true
        end
      end
    end
  end

  it "unlocks orphaned prefetched_jobs" do
    job1 = Delayed::Job.new(:tag => 'tag')
    job2 = Delayed::Job.new(:tag => 'tag')

    job1.create_and_lock!("prefetch:a")
    job1.locked_at = Delayed::Job.db_time_now - 15 * 60
    job1.save!
    job2.create_and_lock!("prefetch:a")

    expect(Delayed::Job.unlock_orphaned_prefetched_jobs).to eq 1
    expect(Delayed::Job.unlock_orphaned_prefetched_jobs).to eq 0

    expect(Delayed::Job.find(job1.id).locked_by).to be_nil
    expect(Delayed::Job.find(job2.id).locked_by).to eq 'prefetch:a'
  end

  it "gets process ids from locked_by" do
    3.times.map { Delayed::Job.create :payload_object => SimpleJob.new }
    locked_jobs = Delayed::Job.get_and_lock_next_available(['job42:2', 'job42:9001'])
    expect(Delayed::Job.processes_locked_locally(name: 'job42').sort).to eq [2, 9001]
    expect(Delayed::Job.processes_locked_locally(name: 'jobnotme')).to be_empty
  end

  it "allows fetching multiple jobs at once" do
    jobs = 3.times.map { Delayed::Job.create :payload_object => SimpleJob.new }
    locked_jobs = Delayed::Job.get_and_lock_next_available(['worker1', 'worker2'])
    locked_jobs.length.should == 2
    locked_jobs.keys.should == ['worker1', 'worker2']
    locked_jobs.values.should == jobs[0..1]
    jobs.map(&:reload).map(&:locked_by).should == ['worker1', 'worker2', nil]
  end

  it "allows fetching extra jobs" do
    jobs = 5.times.map { Delayed::Job.create :payload_object => SimpleJob.new }
    locked_jobs = Delayed::Job.get_and_lock_next_available(['worker1'],
                                                           prefetch: 2,
                                                           prefetch_owner: 'work_queue')
    expect(locked_jobs.length).to eq 2
    expect(locked_jobs.keys).to eq ['worker1', 'work_queue']
    expect(locked_jobs['worker1']).to eq jobs[0]
    expect(locked_jobs['work_queue']).to eq jobs[1..2]
    jobs.map(&:reload).map(&:locked_by).should == ['worker1', 'work_queue', 'work_queue', nil, nil]
  end


  it "should not find jobs scheduled for now when we have forced latency" do
    job = create_job
    Delayed::Job.get_and_lock_next_available('worker', forced_latency: 60.0).should be_nil
    Delayed::Job.get_and_lock_next_available('worker').should eq job
  end

  context "non-transactional", non_transactional: true do
    it "creates a stranded job in a single statement" do
      skip "Requires Rails 5.2 or greater" unless Rails.version >= '5.2'

      allow(Delayed::Job.connection).to receive(:prepared_statements).and_return(false)
      allow(Delayed::Job.connection).to receive(:execute).with(be_include("pg_advisory_xact_lock"), anything).and_call_original.once
      allow(Delayed::Job.connection).to receive(:insert).never
      j = create_job(strand: "test1")
      allow(Delayed::Job.connection).to receive(:execute).and_call_original
      expect(Delayed::Job.find(j.id)).to eq j
    end

    it "creates a non-stranded job in a single statement" do
      skip "Requires Rails 5.2 or greater" unless Rails.version >= '5.2'

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
  end
end
