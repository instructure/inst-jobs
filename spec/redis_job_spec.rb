require File.expand_path("../spec_helper", __FILE__)

describe 'Delayed::Backend::Redis::Job' do
  before :all do
    Delayed.select_backend(Delayed::Backend::Redis::Job)
  end

  after :all do
    Delayed.send(:remove_const, :Job)
  end

  before do
    Delayed::Testing.clear_all!
  end

  include_examples 'a delayed_jobs implementation'

  describe "tickle_strand" do
    it "should continue trying to tickle until the strand is empty" do
      jobs = []
      3.times { jobs << "test".send_later_enqueue_args(:to_s, :strand => "s1", :no_delay => true) }
      job = "test".send_later_enqueue_args(:to_s, :strand => "s1", :no_delay => true)
      # manually delete the first jobs, bypassing the strand book-keeping
      jobs.each { |j| Delayed::Job.redis.del(Delayed::Job::Keys::JOB[j.id]) }
      Delayed::Job.redis.llen(Delayed::Job::Keys::STRAND['s1']).should == 4
      job.destroy
      Delayed::Job.redis.llen(Delayed::Job::Keys::STRAND['s1']).should == 0
    end

    it "should tickle until it finds an existing job" do
      jobs = []
      3.times { jobs << "test".send_later_enqueue_args(:to_s, :strand => "s1", :no_delay => true) }
      job = "test".send_later_enqueue_args(:to_s, :strand => "s1", :no_delay => true)
      # manually delete the first jobs, bypassing the strand book-keeping
      jobs[0...-1].each { |j| Delayed::Job.redis.del(Delayed::Job::Keys::JOB[j.id]) }
      Delayed::Job.redis.llen(Delayed::Job::Keys::STRAND['s1']).should == 4
      jobs[-1].destroy
      Delayed::Job.redis.lrange(Delayed::Job::Keys::STRAND['s1'], 0, -1).should == [job.id]
      found = [Delayed::Job.get_and_lock_next_available('test worker'),
               Delayed::Job.get_and_lock_next_available('test worker')]
      found.should =~ [job, nil]
    end
  end

  describe "missing jobs in queues" do
    before do
      @job = "test".send_later_enqueue_args(:to_s, :no_delay => true)
      @job2 = "test".send_later_enqueue_args(:to_s, :no_delay => true)
      # manually delete the job from redis
      Delayed::Job.redis.del(Delayed::Job::Keys::JOB[@job.id])
    end

    it "should discard when trying to lock" do
      found = [Delayed::Job.get_and_lock_next_available("test worker"),
               Delayed::Job.get_and_lock_next_available("test worker")]
      found.should =~ [@job2, nil]
    end

    it "should filter for find_available" do
      found = [Delayed::Job.find_available(1),
               Delayed::Job.find_available(1)]
      found.should be_include([@job2])
    end
  end

  describe "send_later" do
    it "should schedule job on transaction commit" do
      before_count = Delayed::Job.jobs_count(:current)
      ActiveRecord::Base.transaction do
        job = "string".send_later :reverse
        job.should be_nil
        Delayed::Job.jobs_count(:current).should == before_count
      end
      Delayed::Job.jobs_count(:current) == before_count + 1
    end
  end

  context 'n_strand' do
    it "should default to 1" do
      expect(Delayed::Job).to receive(:rand).never
      job = Delayed::Job.enqueue(SimpleJob.new, :n_strand => 'njobs')
      job.strand.should == "njobs"
    end

    it "should pick a strand randomly out of N" do
      change_setting(Delayed::Settings, :num_strands, ->(strand_name) { expect(strand_name).to eql "njobs"; "3" }) do
        expect(Delayed::Job).to receive(:rand).with(3).and_return(1)
        job = Delayed::Job.enqueue(SimpleJob.new, :n_strand => 'njobs')
        job.strand.should == "njobs:2"
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
          expect(Delayed::Job).to receive(:rand).with(3).and_return(1)
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "123"])
          job.strand.should == "njobs/123:2"
        end
      end

      it "should allow overridding the setting based on the second param" do
        change_setting(Delayed::Settings, :num_strands, ->(strand_name) {
          case strand_name
          when "njobs/123"; 5
          else nil
          end
        }) do
          expect(Delayed::Job).to receive(:rand).with(5).and_return(3)
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "123"])
          job.strand.should == "njobs/123:4"
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "456"])
          job.strand.should == "njobs/456"
        end

        change_setting(Delayed::Settings, :num_strands, ->(strand_name) {
          case strand_name
          when "njobs/123"; 5
          when "njobs"; 3
          else nil
          end
        }) do
          expect(Delayed::Job).to receive(:rand).with(5).and_return(2)
          expect(Delayed::Job).to receive(:rand).with(3).and_return(1)
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "123"])
          job.strand.should == "njobs/123:3"
          job = Delayed::Job.enqueue(SimpleJob.new, n_strand: ["njobs", "456"])
          job.strand.should == "njobs/456:2"
        end
      end
    end
  end
end
