# frozen_string_literal: true

require "spec_helper"
require "delayed/server"

RSpec.describe Delayed::Server, sinatra: true do
  include Rack::Test::Methods

  @update = false

  def app
    described_class.new(update: @update)
  end

  def parsed_body
    @parsed_body ||= JSON.parse(last_response.body)
  end

  before :all do
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
  end

  describe "get '/running'" do
    before do
      3.times do |i|
        Delayed::Job.create!({
                               run_at: Time.zone.now,
                               locked_at: Time.zone.now,
                               locked_by: "dummy-runner-#{i}:${$$}"
                             })
      end
      get "/running"
    end

    it "must return a json object with the running job data in an array", aggregate_failures: true do
      expect(last_response).to be_ok
      expect(parsed_body["data"]).to be_an Array
      expect(parsed_body["data"].size).to eq 3
    end
  end

  describe "get '/jobs'" do
    let!(:job1) { Delayed::Job.enqueue(SimpleJob.new, strand: "strand-1") }
    let!(:job2) { Delayed::Job.enqueue(SimpleJob.new, strand: "strand-2") }
    let!(:job3) { Delayed::Job.enqueue(SimpleJob.new, strand: "strand-3") }

    context "with the flavor param set to id" do
      before do
        get "/jobs?flavor=id&search_term=#{job2.id}"
      end

      it "must only return the job with the id specified in the search_term param" do
        jobs = parsed_body["data"]
        job_ids = jobs.map { |j| j["id"] }
        expect(job_ids).to eq [job2.id]
      end

      it "must set recordsFiltered in the response to 1" do
        expect(parsed_body["recordsFiltered"]).to eq 1
      end
    end
  end

  describe "post '/bulk_update'" do
    let!(:job1) { Delayed::Job.enqueue(SimpleJob.new, strand: "strand-1") }
    let!(:job2) { Delayed::Job.enqueue(SimpleJob.new, strand: "strand-2") }
    let!(:job3) { Delayed::Job.enqueue(SimpleJob.new, strand: "strand-3") }

    context "with update enabled" do
      before do
        @update = true
        post "/bulk_update", JSON.generate(action: "destroy", ids: [job1.id])
      end

      it "must remove job1" do
        expect { Delayed::Job.find(job1.id) }.to raise_error(ActiveRecord::RecordNotFound)
        expect(Delayed::Job.find(job2.id)).not_to be_nil
        expect(Delayed::Job.find(job3.id)).not_to be_nil
      end

      it "must return ok" do
        expect(last_response.ok?).to be true
      end
    end

    context "with update disabled" do
      before do
        @update = false
        post "/bulk_update", JSON.generate(action: "destroy", ids: [job1.id])
      end

      it "must not remove job1" do
        expect(Delayed::Job.find(job1.id)).not_to be_nil
        expect(Delayed::Job.find(job2.id)).not_to be_nil
        expect(Delayed::Job.find(job3.id)).not_to be_nil
      end

      it "must return forbidden" do
        expect(last_response.forbidden?).to be true
      end
    end
  end
end
