# frozen_string_literal: true

module UnlessInJob
  class << self
    attr_accessor :runs

    def run
      self.runs += 1
    end

    def run_later
      delay(synchronous: Delayed::Job.in_delayed_job?).run
    end
  end
end

shared_examples_for "random ruby objects" do
  def set_queue(name) # rubocop:disable Naming/AccessorMethodName
    old_name = Delayed::Settings.queue
    Delayed::Settings.queue = name
  ensure
    Delayed::Settings.queue = old_name
  end

  it "respond_toes :delay method" do
    Object.new.respond_to?(:delay)
  end

  it "raises a ArgumentError if delay is called but the target method doesn't exist" do
    expect { Object.new.delay.method_that_deos_not_exist }.to raise_error(NoMethodError)
  end

  it "adds a new entry to the job table when delay is called on it" do
    expect { Object.new.delay.to_s }.to change { Delayed::Job.jobs_count(:current) }.by(1)
  end

  it "adds a new entry to the job table when delay is called on it with a queue" do
    expect { Object.new.delay(queue: "testqueue").to_s }.to change {
                                                              Delayed::Job.jobs_count(:current, "testqueue")
                                                            }.by(1)
  end

  it "adds a new entry to the job table when delay is called on the class" do
    expect { Object.delay.to_s }.to change { Delayed::Job.jobs_count(:current) }.by(1)
  end

  it "adds a new entry to the job table when delay is called on the class with a queue" do
    expect { Object.delay(queue: "testqueue").to_s }.to change { Delayed::Job.jobs_count(:current, "testqueue") }.by(1)
  end

  context "class methods" do
    context "handle_asynchronously" do
      it "works with default_async" do
        klass = Class.new do
          attr_reader :ran

          def test_method
            @ran = true
          end
          handle_asynchronously :test_method
        end
        obj = klass.new
        expect { obj.test_method }.to change { Delayed::Job.jobs_count(:current) }.by(1)
        expect(obj.ran).to be_falsey
        expect { obj.test_method(synchronous: true) }.not_to(change { Delayed::Job.jobs_count(:current) })
        expect(obj.ran).to be true
      end

      it "must work with enqueue args that are lambdas" do
        klass = Class.new do
          attr_reader :ran

          def test_method
            @ran = true
          end
          handle_asynchronously :test_method, singleton: ->(obj) { "foobar:#{obj.object_id}" }
        end

        obj = klass.new
        expect { obj.test_method }.to change { Delayed::Job.jobs_count(:current) }.by(1)
      end

      it "must work with kwargs in the original method" do
        klass = Class.new do
          attr_reader :run

          def test_method(my_kwarg: nil)
            @run = my_kwarg
          end
          handle_asynchronously :test_method

          def other_test(arg)
            @foo = arg
          end
          handle_asynchronously :other_test
        end

        obj = klass.new
        obj.test_method(my_kwarg: "foo", synchronous: true)
        expect(obj.run).to eq "foo"
      end

      it "sends along enqueue args and args" do
        klass = Class.new do
          attr_accessor :ran

          def test_method(*args)
            @ran = args
          end
          handle_asynchronously(:test_method, enqueue_arg1: :thing)
        end
        obj = klass.new
        method = double

        expect(Delayed::PerformableMethod).to receive(:new)
          .with(obj,
                :test_method,
                args: [1, 2, 3],
                kwargs: { synchronous: true },
                on_failure: nil,
                on_permanent_failure: nil,
                sender: obj)
          .and_return(method)
        expect(Delayed::Job).to receive(:enqueue).with(method, enqueue_arg1: :thing)
        obj.test_method(1, 2, 3)

        expect(Delayed::PerformableMethod).to receive(:new)
          .with(obj,
                :test_method,
                args: [4],
                kwargs: { synchronous: true },
                on_failure: nil,
                on_permanent_failure: nil,
                sender: obj)
          .and_return(method)
        expect(Delayed::Job).to receive(:enqueue).with(method, enqueue_arg1: :thing)
        obj.test_method(4)

        expect(obj.ran).to be_nil
        obj.test_method(7, synchronous: true)
        expect(obj.ran).to eq([7])
        obj.ran = nil
        expect(obj.ran).to be_nil
        obj.test_method(8, 9, synchronous: true)
        expect(obj.ran).to eq([8, 9])
      end

      it "handles punctuation correctly" do
        klass = Class.new do
          attr_reader :ran

          def test_method?
            @ran = true
          end
          handle_asynchronously :test_method?
        end
        obj = klass.new
        expect { obj.test_method? }.to change { Delayed::Job.jobs_count(:current) }.by(1)
        expect(obj.ran).to be_falsey
        expect { obj.test_method?(synchronous: true) }.not_to(change { Delayed::Job.jobs_count(:current) })
        expect(obj.ran).to be true
      end

      it "handles assignment punctuation correctly" do
        klass = Class.new do
          attr_reader :ran

          def test_method=(val)
            @ran = val
          end
          handle_asynchronously :test_method=
        end
        obj = klass.new
        expect { obj.test_method = 3 }.to change { Delayed::Job.jobs_count(:current) }.by(1)
        expect(obj.ran).to be_nil
        expect { obj.send(:test_method=, 5, synchronous: true) }.not_to(change { Delayed::Job.jobs_count(:current) })
        expect(obj.ran).to eq(5)
      end

      it "correctlies sort out method accessibility" do
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

        expect(klass1.public_method_defined?(:test_method)).to be true
        expect(klass2.protected_method_defined?(:test_method)).to be true
        expect(klass3.private_method_defined?(:test_method)).to be true
      end
    end
  end

  it "calls send later on methods which are wrapped with handle_asynchronously" do
    story = Story.create text: "Once upon..."

    expect { story.whatever(1, 5) }.to change { Delayed::Job.jobs_count(:current) }.by(1)

    job = Delayed::Job.list_jobs(:current, 1).first
    expect(job.payload_object.class).to   eq(Delayed::PerformableMethod)
    expect(job.payload_object.method).to  eq(:whatever)
    expect(job.payload_object.args).to    eq([1, 5])
    expect(job.payload_object.kwargs).to  eq({ synchronous: true })
    expect(job.payload_object.perform).to eq("Once upon...")
  end

  context "delay" do
    it "uses the default queue if there is one" do
      set_queue("testqueue") do
        "string".delay.reverse
        job = Delayed::Job.list_jobs(:current, 1).first
        expect(job.queue).to eq("testqueue")

        "string".delay(queue: nil).reverse
        job2 = Delayed::Job.list_jobs(:current, 2).last
        expect(job2.queue).to eq("testqueue")
      end
    end

    it "requires a queue" do
      expect { set_queue(nil) }.to raise_error(ArgumentError)
    end
  end

  context "delay with run_at" do
    it "queues a new job" do
      expect do
        "string".delay(run_at: 1.hour.from_now).length
      end.to change { Delayed::Job.jobs_count(:future) }.by(1)
    end

    it "schedules the job in the future" do
      time = 1.hour.from_now
      "string".delay(run_at: time).length
      job = Delayed::Job.list_jobs(:future, 1).first
      expect(job.run_at.to_i).to eq(time.to_i)
    end

    it "stores payload as PerformableMethod" do
      "string".delay(run_at: 1.hour.from_now).count("r")
      job = Delayed::Job.list_jobs(:future, 1).first
      expect(job.payload_object.class).to   eq(Delayed::PerformableMethod)
      expect(job.payload_object.method).to  eq(:count)
      expect(job.payload_object.args).to    eq(["r"])
      expect(job.payload_object.perform).to eq(1)
    end

    it "uses the default queue if there is one" do
      set_queue("testqueue") do
        "string".delay(run_at: 1.hour.from_now).reverse
        job = Delayed::Job.list_jobs(:current, 1).first
        expect(job.queue).to eq("testqueue")
      end
    end
  end

  describe "delay with synchronous argument" do
    before do
      UnlessInJob.runs = 0
    end

    it "performs immediately if in job" do
      UnlessInJob.delay.run_later
      job = Delayed::Job.list_jobs(:current, 1).first
      job.invoke_job
      expect(UnlessInJob.runs).to eq(1)
    end

    it "queues up for later if not in job" do
      UnlessInJob.run_later
      expect(UnlessInJob.runs).to eq(0)
    end
  end
end
