# frozen_string_literal: true

require 'spec_helper'
require 'imperium'

RSpec.describe Delayed::Worker::ConsulHealthCheck do
  let(:health_check) { Delayed::Worker::ConsulHealthCheck.new(worker_name: 'foobar') }

  # can't use a verifying double for the response because the methods we're
  # tryig to stub are actually on HTTP::Message
  let(:response) { double('Imperium::Response') }
  let(:agent_client) { instance_double(Imperium::Agent) }

  before do
    allow(Imperium::Agent).to receive(:default_client).and_return(agent_client)
  end

  describe '#initialize' do
    it 'must use the default agent client when the config is mostly empty' do
      check = Delayed::Worker::ConsulHealthCheck.new({worker_name: 'foobar'})
      expect(check.agent_client).to eq Imperium::Agent.default_client
    end

    it 'must create a new agent API client when the config has relevant keys set' do
      check = Delayed::Worker::ConsulHealthCheck.new(worker_name: 'foobar', config: {url: 'http://consul.example.com:8500'})
      agent_client = check.agent_client
      expect(agent_client).to_not eq Imperium::Agent.default_client
      expect(agent_client.config.url.to_s).to eq 'http://consul.example.com:8500'
    end
  end

  describe '#start' do
    it 'must register this process as a service with consul' do
      expect(response).to receive(:ok?).and_return(true)
      expect(agent_client).to receive(:register_service)
        .with(an_instance_of(Imperium::Service))
        .and_return(response)
      health_check.start
    end


    it 'must supply a args style check' do
      allow(response).to receive(:ok?).and_return(true)
      allow(agent_client).to receive(:register_service) { |service|
        check = service.checks.first
        expect(check.args).to_not be_nil
        response
      }
      health_check.start
    end

    it 'must include the docker container id when the docker option is set to true' do
      local_health_check = Delayed::Worker::ConsulHealthCheck.new(
        worker_name: 'foobar',
        config: {docker: true}
      )
      allow(response).to receive(:ok?).and_return(true)
      allow(agent_client).to receive(:register_service) { |service|
        check = service.checks.first
        expect(check.docker_container_id).to_not be_nil
        response
      }
      local_health_check.start
    end
  end

  describe '#stop' do
    it 'must deregister the service from consul' do
      allow(response).to receive(:ok?).and_return(true)
      expect(agent_client).to receive(:deregister_service)
        .with(health_check.worker_name)
        .and_return(response)
      health_check.stop
    end
  end
end
