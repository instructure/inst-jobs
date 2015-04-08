require 'spec_helper'
require 'delayed/server'

RSpec.describe Delayed::Server, sinatra: true do
  include Rack::Test::Methods

  def app
    described_class.new
  end

  before :all do
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
  end

  after :all do
    Delayed.send(:remove_const, :Job)
  end

  describe "get '/running'" do
    before do
      3.times do |i|
        Delayed::Job.create!({
          run_at: Time.now,
          locked_at: Time.now,
          locked_by: "dummy-runner-#{i}:${$$}",
        })
      end
      get '/running'
    end

    it 'must return a json object with the running job data in an array', aggregate_failures: true do
      expect(last_response).to be_ok
      parsed_body = JSON.parse(last_response.body)
      expect(parsed_body['data']).to be_an Array
      expect(parsed_body['data'].size).to eq 3
    end
  end
end
