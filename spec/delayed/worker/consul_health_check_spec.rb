# frozen_string_literal: true

require "spec_helper"

RSpec.describe Delayed::Worker::ConsulHealthCheck do
  let(:health_check) { described_class.new(worker_name: "foobar") }

  describe "#initialize" do
    it "must use a default service client when the config is mostly empty" do
      check = described_class.new(worker_name: "foobar")
      expect(check.service_client.configuration.url.to_s).to eq "http://localhost:8500"
    end

    it "must create a new service API client when the config has relevant keys set" do
      check = described_class.new(worker_name: "foobar",
                                  config: { url: "http://consul.example.com:8500" })
      service_client = check.service_client
      expect(service_client.configuration.url.to_s).to eq "http://consul.example.com:8500"
    end
  end

  describe "#start" do
    it "must register this process as a service with consul" do
      stub = stub_request(:put, "localhost:8500/v1/agent/service/register")
             .with(body: hash_including({ id: "foobar" }))

      health_check.start

      expect(stub).to have_been_requested
    end

    it "must supply a args style check" do
      stub = stub_request(:put, "localhost:8500/v1/agent/service/register")
             .with(body: hash_including({ check:  WebMock::API.hash_including({ args: anything }) }))

      health_check.start

      expect(stub).to have_been_requested
    end

    it "must include the docker container id when the docker option is set to true" do
      container_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
      allow(File).to receive(:read).and_call_original
      allow(File).to receive(:read).with("/proc/1/cgroup").and_return(<<~TEXT)
        12:pids:/docker/#{container_id}
        11:memory:/docker/#{container_id}
        1:name=systemd:/docker/#{container_id}
      TEXT

      stub = stub_request(:put, "localhost:8500/v1/agent/service/register")
             .with(body: hash_including({ check:  WebMock::API.hash_including({ docker_container_id: container_id }) }))

      local_health_check = described_class.new(
        worker_name: "foobar",
        config: { docker: true }
      )
      local_health_check.start

      expect(stub).to have_been_requested
    end

    it "must not start a heartbeat thread for a script check" do
      stub_request(:put, "localhost:8500/v1/agent/service/register")

      health_check.start

      expect(health_check.instance_variable_get(:@heartbeat_thread)).to be_nil
    end
  end

  context "with a ttl check" do
    let(:ttl_health_check) do
      described_class.new(worker_name: "foobar", config: { check_type: "ttl", ttl: 30 })
    end
    let(:heartbeat_thread) { ttl_health_check.instance_variable_get(:@heartbeat_thread) }

    before { stub_request(:put, "localhost:8500/v1/agent/service/register") }
    after { heartbeat_thread&.kill }

    it "must register a ttl check instead of a script check" do
      expected_check = { "ttl" => "30s", "status" => "passing", "deregister_critical_service_after" => "10m" }
      stub = stub_request(:put, "localhost:8500/v1/agent/service/register")
             .with { |request| JSON.parse(request.body)["check"] == expected_check }

      ttl_health_check.start

      expect(stub).to have_been_requested
    end

    it "must start a heartbeat thread" do
      ttl_health_check.start

      expect(heartbeat_thread).to be_alive
    end

    it "must pass the check on each heartbeat" do
      stub = stub_request(:put, "localhost:8500/v1/agent/check/update/service:foobar")
             .with(body: hash_including({ Status: "passing" }))

      ttl_health_check.send(:heartbeat)

      expect(stub).to have_been_requested
    end

    it "must register again when the agent has forgotten the check" do
      stub_request(:put, "localhost:8500/v1/agent/check/update/service:foobar").to_return(status: 404)
      register = stub_request(:put, "localhost:8500/v1/agent/service/register")

      ttl_health_check.send(:heartbeat)

      expect(register).to have_been_requested
    end

    it "must not register again when the agent is unavailable" do
      stub_request(:put, "localhost:8500/v1/agent/check/update/service:foobar").to_return(status: 500)
      register = stub_request(:put, "localhost:8500/v1/agent/service/register")

      expect { ttl_health_check.send(:heartbeat) }.to raise_error(Faraday::ServerError)
      expect(register).not_to have_been_requested
    end

    it "must stop the heartbeat thread and deregister on stop" do
      deregister = stub_request(:put, "localhost:8500/v1/agent/service/deregister/foobar")
      ttl_health_check.start
      thread = heartbeat_thread # stop clears it

      ttl_health_check.stop

      expect(thread.join(1)).to eq thread
      expect(deregister).to have_been_requested
    end
  end

  describe "#stop" do
    it "must deregister the service from consul" do
      stub = stub_request(:put, "localhost:8500/v1/agent/service/deregister/foobar")

      health_check.stop
      expect(stub).to have_been_requested
    end
  end
end
