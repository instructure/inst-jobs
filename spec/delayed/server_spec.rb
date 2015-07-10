require 'spec_helper'
require 'delayed/server'

RSpec.describe Delayed::Server, sinatra: true do
  include Rack::Test::Methods

  def app
    described_class.new
  end

  def parsed_body
    @parsed_body ||= JSON.parse(last_response.body)
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
      expect(parsed_body['data']).to be_an Array
      expect(parsed_body['data'].size).to eq 3
    end
  end

  describe "get '/jobs'" do
    let!(:job_1) { Delayed::Job.enqueue(SimpleJob.new, strand: 'strand-1') }
    let!(:job_2) { Delayed::Job.enqueue(SimpleJob.new, strand: 'strand-2') }
    let!(:job_3) { Delayed::Job.enqueue(SimpleJob.new, strand: 'strand-3') }

    context 'with the flavor param set to id' do
      before do
        get "/jobs?flavor=id&search_term=#{job_2.id}"
      end

      it 'must only return the job with the id specified in the search_term param' do
        jobs = parsed_body['data']
        job_ids = jobs.map{ |j| j['id'] }
        expect(job_ids).to eq [job_2.id]
      end

      it 'must set recordsFiltered in the response to 1' do
        expect(parsed_body['recordsFiltered']).to eq 1
      end
    end
  end
end
