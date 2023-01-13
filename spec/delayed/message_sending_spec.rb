# frozen_string_literal: true

require "spec_helper"
require "debug_inspector"

RSpec.describe Delayed::MessageSending do
  before do
    allow(Rails.env).to receive(:test?).and_return(true)
  end

  before(:all) do
    # this has to be a "real" constant
    class SpecClass # rubocop:disable RSpec/LeakyConstantDeclaration, Lint/ConstantDefinitionInBlock
      def call_private(**enqueue_args)
        delay(**enqueue_args).private_method
      end

      def call_protected(**enqueue_args)
        other = self.class.new
        other.delay(**enqueue_args).protected_method
      end

      def call_public(**_kwargs)
        42
      end

      private

      def private_method; end

      protected

      def protected_method; end
    end
  end

  after(:all) do
    Object.send(:remove_const, :SpecClass)
  end

  let(:klass) { SpecClass }

  it "allows an object to send a private message to itself" do
    expect do
      job = klass.new.call_private(ignore_transaction: true)
      job.invoke_job
    end.not_to raise_error
  end

  it "allows an object to send a private message to itself synchronouosly" do
    expect { klass.new.call_private(synchronous: true) }.not_to raise_error
  end

  it "warns about directly sending a private message asynchronously" do
    expect { klass.new.delay.private_method }.to raise_error(NoMethodError)
  end

  it "warns about directly sending a private message synchronusly" do
    expect { klass.new.delay(synchronous: true).private_method }.to raise_error(NoMethodError)
  end

  it "does not warn about directly sending a private message in production" do
    allow(Rails.env).to receive(:test?).and_return(false)
    allow(Rails.env).to receive(:development?).and_return(false)
    expect { klass.new.delay.private_method }.not_to raise_error
  end

  it "does not warn about directly sending a private message synchronously in production" do
    allow(Rails.env).to receive(:test?).and_return(false)
    allow(Rails.env).to receive(:development?).and_return(false)
    expect { klass.new.delay(synchronous: true).private_method }.not_to raise_error
  end

  it "allows an object to send a protected message to itself" do
    job = klass.new.call_protected(ignore_transaction: true)
    expect { job.invoke_job }.not_to raise_error
  end

  it "allows an object to send a protected message to itself synchronouosly" do
    expect { klass.new.call_protected(synchronous: true) }.not_to raise_error
  end

  it "directly calls a public method on an object with kwargs" do
    expect(klass.new.delay(synchronous: true).call_public(kwarg: 10)).to eq 42
  end

  it "warns about directly sending a protected message asynchronously" do
    expect { klass.new.delay.protected_method }.to raise_error(NoMethodError)
  end

  it "warns about directly sending a protected message synchronusly" do
    expect { klass.new.delay(synchronous: true).protected_method }.to raise_error(NoMethodError)
  end

  it "doesn't explode if you can't dump the sender" do
    klass = Class.new do
      def delay_something
        Kernel.delay.sleep(1)
      end

      def encode_with(_encoder)
        raise "yaml encoding failed"
      end
    end

    obj = klass.new
    expect { YAML.dump(obj) }.to raise_error("yaml encoding failed")
    expect { obj.delay_something }.not_to raise_error
  end
end
