shared_examples_for 'random ruby objects' do
  def set_queue(name)
    old_name = Delayed::Settings.queue
    Delayed::Settings.queue = name
  ensure
    Delayed::Settings.queue = old_name
  end

  it "should respond_to :delay method" do
    Object.new.respond_to?(:delay)
  end

  it "should raise a ArgumentError if delay is called but the target method doesn't exist" do
    lambda { Object.new.delay.method_that_deos_not_exist }.should raise_error(NoMethodError)
  end

  it "should add a new entry to the job table when delay is called on it" do
    lambda { Object.new.delay.to_s }.should change { Delayed::Job.jobs_count(:current) }.by(1)
  end

  it "should add a new entry to the job table when delay is called on it with a queue" do
    lambda { Object.new.delay(queue: "testqueue").to_s }.should change { Delayed::Job.jobs_count(:current, "testqueue") }.by(1)
  end

  it "should add a new entry to the job table when delay is called on the class" do
    lambda { Object.delay.to_s }.should change { Delayed::Job.jobs_count(:current) }.by(1)
  end

  it "should add a new entry to the job table when delay is called on the class with a queue" do
    lambda { Object.delay(queue: "testqueue").to_s }.should change { Delayed::Job.jobs_count(:current, "testqueue") }.by(1)
  end

  context "class methods" do
    context "handle_asynchronously" do
      it "should work with default_async" do
        klass = Class.new do
          attr_reader :ran
          def test_method; @ran = true; end
          handle_asynchronously :test_method
        end
        obj = klass.new
        lambda { obj.test_method }.should change { Delayed::Job.jobs_count(:current) }.by(1)
        obj.ran.should be_falsey
        lambda { obj.test_method(synchronous: true) }.should_not change { Delayed::Job.jobs_count(:current) }
        obj.ran.should be true
      end

      it 'must work with enqueue args that are lambdas' do
        klass = Class.new do
          attr_reader :ran
          def test_method; @ran = true; end
          handle_asynchronously :test_method, singleton: -> (obj) { "foobar:#{obj.object_id}" }
        end

        obj = klass.new
        lambda { obj.test_method }.should change { Delayed::Job.jobs_count(:current) }.by(1)
      end

      it 'must work with kwargs in the original method' do
        klass = Class.new do
          attr_reader :run
          def test_method(my_kwarg: nil); @run = my_kwarg; end
          handle_asynchronously :test_method

          def other_test(arg); @foo = arg; end
          handle_asynchronously :other_test
        end

        obj = klass.new
        obj.test_method(my_kwarg: 'foo', synchronous: true)
        expect(obj.run).to eq 'foo'
      end

      it "should send along enqueue args and args" do
        klass = Class.new do
          attr_accessor :ran
          def test_method(*args); @ran = args; end
          handle_asynchronously(:test_method, enqueue_arg_1: :thing)
        end
        obj = klass.new
        method = double()

        expect(Delayed::PerformableMethod).to receive(:new).with(obj, :test_method, args: [1,2,3], kwargs: {synchronous: true}, on_failure: nil, on_permanent_failure: nil).and_return(method)
        expect(Delayed::Job).to receive(:enqueue).with(method, :enqueue_arg_1 => :thing)
        obj.test_method(1,2,3)

        expect(Delayed::PerformableMethod).to receive(:new).with(obj, :test_method, args: [4], kwargs: {:synchronous=>true}, on_failure: nil, on_permanent_failure: nil).and_return(method)
        expect(Delayed::Job).to receive(:enqueue).with(method, :enqueue_arg_1 => :thing)
        obj.test_method(4)

        obj.ran.should be_nil
        obj.test_method(7, synchronous: true)
        obj.ran.should == [7]
        obj.ran = nil
        obj.ran.should == nil
        obj.test_method(8,9, synchronous: true)
        obj.ran.should == [8,9]
      end

      it "should handle punctuation correctly" do
        klass = Class.new do
          attr_reader :ran
          def test_method?; @ran = true; end
          handle_asynchronously :test_method?
        end
        obj = klass.new
        lambda { obj.test_method? }.should change { Delayed::Job.jobs_count(:current) }.by(1)
        obj.ran.should be_falsey
        lambda { obj.test_method?(synchronous: true) }.should_not change { Delayed::Job.jobs_count(:current) }
        obj.ran.should be true
      end

      it "should handle assignment punctuation correctly" do
        klass = Class.new do
          attr_reader :ran
          def test_method=(val); @ran = val; end
          handle_asynchronously :test_method=
        end
        obj = klass.new
        lambda { obj.test_method = 3 }.should change { Delayed::Job.jobs_count(:current) }.by(1)
        obj.ran.should be_nil
        lambda { obj.send(:test_method=, 5, synchronous: true) }.should_not change { Delayed::Job.jobs_count(:current) }
        obj.ran.should == 5
      end

      it "should correctly sort out method accessibility" do
        klass1 = Class.new do
          def test_method; end
          handle_asynchronously :test_method
        end

        klass2 = Class.new do
          protected
          def test_method; end
          handle_asynchronously :test_method
        end

        klass3 = Class.new do
          private
          def test_method; end
          handle_asynchronously :test_method
        end

        klass1.public_method_defined?(:test_method).should be true
        klass2.protected_method_defined?(:test_method).should be true
        klass3.private_method_defined?(:test_method).should be true
      end
    end
  end

  it "should call send later on methods which are wrapped with handle_asynchronously" do
    story = Story.create :text => 'Once upon...'

    expect { story.whatever(1, 5) }.to change { Delayed::Job.jobs_count(:current) }.by(1)

    job = Delayed::Job.list_jobs(:current, 1).first
    job.payload_object.class.should   == Delayed::PerformableMethod
    job.payload_object.method.should  == :whatever
    job.payload_object.args.should    == [1, 5]
    job.payload_object.kwargs.should  == {:synchronous=>true}
    job.payload_object.perform.should == 'Once upon...'
  end

  context "delay" do
    it "should use the default queue if there is one" do
      set_queue("testqueue") do
        "string".delay.reverse
        job = Delayed::Job.list_jobs(:current, 1).first
        job.queue.should == "testqueue"

        "string".delay(queue: nil).reverse
        job2 = Delayed::Job.list_jobs(:current, 2).last
        job2.queue.should == "testqueue"
      end
    end

    it "should require a queue" do
      expect { set_queue(nil) }.to raise_error(ArgumentError)
    end
  end

  context "delay with run_at" do
    it "should queue a new job" do
      lambda do
        "string".delay(run_at: 1.hour.from_now).length
      end.should change { Delayed::Job.jobs_count(:future) }.by(1)
    end

    it "should schedule the job in the future" do
      time = 1.hour.from_now
      "string".delay(run_at: time).length
      job = Delayed::Job.list_jobs(:future, 1).first
      job.run_at.to_i.should == time.to_i
    end

    it "should store payload as PerformableMethod" do
      "string".delay(run_at: 1.hour.from_now).count('r')
      job = Delayed::Job.list_jobs(:future, 1).first
      job.payload_object.class.should   == Delayed::PerformableMethod
      job.payload_object.method.should  == :count
      job.payload_object.args.should    == ['r']
      job.payload_object.perform.should == 1
    end

    it "should use the default queue if there is one" do
      set_queue("testqueue") do
        "string".delay(run_at: 1.hour.from_now).reverse
        job = Delayed::Job.list_jobs(:current, 1).first
        job.queue.should == "testqueue"
      end
    end
  end

  describe "delay with synchronous argument" do
    module UnlessInJob
      @runs = 0
      def self.runs; @runs; end

      def self.run
        @runs += 1
      end

      def self.run_later
        self.delay(synchronous: Delayed::Job.in_delayed_job?).run
      end
    end

    before do
      UnlessInJob.class_eval { @runs = 0 }
    end

    it "should perform immediately if in job" do
      UnlessInJob.delay.run_later
      job = Delayed::Job.list_jobs(:current, 1).first
      job.invoke_job
      UnlessInJob.runs.should == 1
    end

    it "should queue up for later if not in job" do
      UnlessInJob.run_later
      UnlessInJob.runs.should == 0
    end
  end
end
