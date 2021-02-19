# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Delayed::Worker::ConsulHealthCheck do
  let(:health_check) { Delayed::Worker::ConsulHealthCheck.new(worker_name: 'foobar') }

  describe '#initialize' do
    it 'must use a default service client when the config is mostly empty' do
      check = Delayed::Worker::ConsulHealthCheck.new(worker_name: 'foobar')
      expect(check.service_client.configuration.url.to_s).to eq 'http://localhost:8500'
    end

    it 'must create a new service API client when the config has relevant keys set' do
      check = Delayed::Worker::ConsulHealthCheck.new(worker_name: 'foobar', config: {url: 'http://consul.example.com:8500'})
      service_client = check.service_client
      expect(service_client.configuration.url.to_s).to eq 'http://consul.example.com:8500'
    end
  end

  describe '#start' do
    it 'must register this process as a service with consul' do
      stub = stub_request(:put, "localhost:8500/v1/agent/service/register")
        .with(body: hash_including({id: 'foobar' }))

      health_check.start

      expect(stub).to have_been_requested
    end


    it 'must supply a args style check' do
      stub = stub_request(:put, "localhost:8500/v1/agent/service/register")
        .with(body: hash_including({check:  WebMock::API.hash_including({args: anything})}))

      health_check.start

      expect(stub).to have_been_requested
    end

    it 'must include the docker container id when the docker option is set to true' do
      stub = stub_request(:put, "localhost:8500/v1/agent/service/register")
        .with(body: hash_including({check:  WebMock::API.hash_including({docker_container_id: anything})}))

      local_health_check = Delayed::Worker::ConsulHealthCheck.new(
        worker_name: 'foobar',
        config: {docker: true}
      )
      local_health_check.start

      expect(stub).to have_been_requested
    end
  end

  describe '#stop' do
    it 'must deregister the service from consul' do
      stub = stub_request(:put, "localhost:8500/v1/agent/service/deregister/foobar")

      health_check.stop
      expect(stub).to have_been_requested
    end
  end
end
