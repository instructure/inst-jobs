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
    job = "test".send_later_enqueue_args :reverse, no_delay: true
    job_id = job.id
    proc { job.fail! }.should raise_error
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
      @job.update_attributes(:attempts => 1, :run_at => 1.day.from_now)
      @job_copy_for_worker_2.send(:lock_exclusively!, 'worker2').should == false
    end

    it "should select the next job at random if enabled" do
      begin
        Delayed::Settings.select_random_from_batch = true
        15.times { "test".send_later :length }
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
    job.unlock
    job.save!
    job.reload
    job.locked_by.should == nil
    job.locked_at.should == nil
  end


  describe "bulk_update failed jobs" do
    before do
      @flavor = 'failed'
      @affected_jobs = []
      @ignored_jobs = []
      Timecop.freeze(5.minutes.ago) do
        3.times { @affected_jobs << create_job.tap { |j| j.fail! } }
        @ignored_jobs << create_job(:run_at => 2.hours.from_now)
        @ignored_jobs << create_job(:queue => 'q2')
      end
    end

    it "should raise error when holding failed jobs" do
      expect { Delayed::Job.bulk_update('hold', :flavor => @flavor, :query => @query) }.to raise_error
    end

    it "should raise error unholding holding failed jobs" do
      expect { Delayed::Job.bulk_update('unhold', :flavor => @flavor, :query => @query) }.to raise_error
    end

    it "should delete failed jobs" do
      Delayed::Job.bulk_update('destroy', :flavor => @flavor, :query => @query).should == @affected_jobs.size
      @affected_jobs.map { |j| Delayed::Job.find(j.id) rescue nil }.compact.should be_blank
      @ignored_jobs.map { |j| Delayed::Job.find(j.id) rescue nil }.compact.size.should == @ignored_jobs.size
    end
  end

end
