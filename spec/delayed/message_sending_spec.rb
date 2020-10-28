# frozen_string_literal: true

require 'spec_helper'
require 'debug_inspector'

RSpec.describe Delayed::MessageSending do
  before do
    allow(::Rails.env).to receive(:test?).and_return(true)
  end

  let(:klass) do
    Class.new do
      def call_private(**enqueue_args)
        delay(**enqueue_args).private_method
      end

      private

      def private_method
      end
    end
  end

  it "allows an object to send a private message to itself" do
    klass.new.call_private
  end

  it "allows an object to send a private message to itself synchronouosly" do
    klass.new.call_private(synchronous: true)
  end

  it "warns about directly sending a private message asynchronously" do
    expect { klass.new.delay.private_method }.to raise_error(NoMethodError)
  end

  it "warns about directly sending a private message synchronusly" do
    expect { klass.new.delay(synchronous: true).private_method }.to raise_error(NoMethodError)
  end

  it "does not warn about directly sending a private message in production" do
    allow(::Rails.env).to receive(:test?).and_return(false)
    allow(::Rails.env).to receive(:development?).and_return(false)
    klass.new.delay.private_method
  end

  it "does not warn about directly sending a private message synchronously in production" do
    allow(::Rails.env).to receive(:test?).and_return(false)
    allow(::Rails.env).to receive(:development?).and_return(false)
    klass.new.delay(synchronous: true).private_method
  end
end
