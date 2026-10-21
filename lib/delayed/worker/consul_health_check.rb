# frozen_string_literal: true

require_relative "health_check"
require_relative "process_helper"
require "socket"

module Delayed
  class Worker
    class ConsulHealthCheck < HealthCheck
      include Delayed::Logging

      self.type_name = :consul

      CONSUL_CONFIG_KEYS = %w[url acl_token].map(&:freeze).freeze
      DEFAULT_SERVICE_NAME = "inst-jobs_worker"
      attr_reader :service_client, :health_client, :check_client

      def initialize(*, **)
        super
        # Because we don't want the consul client to be a hard dependency we're
        # only requiring it once it's absolutely needed
        require "diplomat"

        if config.keys.any? { |k| CONSUL_CONFIG_KEYS.include?(k) }
          consul_config = Diplomat::Configuration.new.tap do |conf|
            CONSUL_CONFIG_KEYS.each do |key|
              conf.send(:"#{key}=", config[key]) if config[key]
            end
          end
          @service_client = Diplomat::Service.new(configuration: consul_config)
          @health_client = Diplomat::Health.new(configuration: consul_config)
          @check_client = Diplomat::Check.new(configuration: consul_config)
        else
          @service_client = Diplomat::Service.new
          @health_client = Diplomat::Health.new
          @check_client = Diplomat::Check.new
        end
      end

      def start
        registered = register
        start_heartbeat if registered && ttl_check?
        registered
      end

      def stop
        @heartbeat_thread&.kill
        @heartbeat_thread = nil
        @service_client.deregister(worker_name)
      end

      def live_workers
        # Filter out critical workers (probably nodes failing their serf health check)
        live_nodes = @health_client.service(service_name, {
                                              filter: "not Checks.Status == critical"
                                            })

        live_nodes.map { |n| n.Service["ID"] }
      end

      private

      def register
        @service_client.register({
                                   id: worker_name,
                                   name: service_name,
                                   check: check_attributes
                                 })
      end

      def ttl_check?
        @config.fetch(:check_type, "script").to_s == "ttl"
      end

      def check_id
        "service:#{worker_name}"
      end

      # in seconds
      def ttl
        Integer(@config.fetch(:ttl, 60))
      end

      def start_heartbeat
        @heartbeat_thread = Thread.new do
          loop do
            sleep(ttl / 3.0)
            heartbeat
          rescue => e
            logger.warn("Consul TTL update failed for #{worker_name}: #{e.class}: #{e.message}")
          end
        end
      end

      def heartbeat
        @check_client.pass(check_id)
      rescue Faraday::ResourceNotFound
        logger.warn("Consul forgot #{worker_name}, re-registering")
        register
      end

      def check_attributes
        base = {
          status: "passing",
          deregister_critical_service_after: @config.fetch(:deregister_service_delay, "10m")
        }
        return base.merge(ttl: "#{ttl}s") if ttl_check?

        base.merge(
          args: ["bash", "-c", check_script],
          interval: @config.fetch(:check_interval, "5m")
        ).tap do |h|
          h[:docker_container_id] = docker_container_id if @config["docker"]
        end
      end

      def check_script
        return @check_script if @check_script

        mtime = ProcessHelper.mtime(Process.pid)
        @check_script = ProcessHelper.check_script(Process.pid, mtime)
      end

      # This method is horrible, it takes advantage of the fact that docker uses
      # cgroups for part of its magic and also uses the container id as the cgroup name
      def docker_container_id
        return @docker_container_id if @docker_container_id

        content = File.read("/proc/1/cgroup").split("\n")
        @docker_container_id = content.last.split("/").last
      end

      def service_name
        @service_name ||= @config.fetch("service_name", DEFAULT_SERVICE_NAME)
      end
    end
  end
end
