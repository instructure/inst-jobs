require_relative 'health_check'
require 'socket'

module Delayed
  class Worker
    class ConsulHealthCheck < HealthCheck
      self.type_name = :consul

      CONSUL_CONFIG_KEYS = %w{url host port ssl token connect_timeout receive_timeout send_timeout}.map(&:freeze).freeze
      DEFAULT_SERVICE_NAME = 'inst-jobs_worker'.freeze
      attr_reader :agent_client, :catalog_client

      SCRIPT_TEMPLATE = <<-BASH.freeze
        WORKER_PID="%<pid>d" # an example, filled from ruby when the check is created
        ORIGINAL_MTIME="%<mtime>d" # an example, filled from ruby when the check is created

        if [ -d "/proc/$WORKER_PID" ]; then
            CURRENT_MTIME=$(stat -f %%Y /proc/$WORKER_PID)

            if [ "$ORIGINAL_MTIME" = "$CURRENT_MTIME" ]; then
                exit 0 # Happy day
            else
                echo "PID still exists but procfs entry has changed, current command:"
                ps -p $PID -o 'command='
                exit 1 # Something is wrong, trigger a "warning" state
            fi
        else
            exit 255 # The process is no more, trigger a "critical" state.
        fi
      BASH

      def initialize(*args)
        super
        # Because we don't want the consul client to be a hard dependency we're
        # only requiring it once it's absolutely needed
        require 'imperium'

        if config.keys.any? { |k| CONSUL_CONFIG_KEYS.include?(k) }
          consul_config = Imperium::Configuration.new.tap do |conf|
            CONSUL_CONFIG_KEYS.each do |key|
              conf.send("#{key}=", config[key]) if config[key]
            end
          end
          @agent_client = Imperium::Agent.new(consul_config)
          @catalog_client = Imperium::Catalog.new(consul_config)
        else
          @agent_client = Imperium::Agent.default_client
          @catalog_client = Imperium::Catalog.default_client
        end
      end

      def start
        service = Imperium::Service.new({
          id: worker_name,
          name: service_name,
        })
        service.add_check(check_attributes)
        response = @agent_client.register_service(service)
        response.ok?
      end

      def stop
        response = @agent_client.deregister_service(worker_name)
        response.ok? || response.not_found?
      end

      def live_workers
        live_nodes = @catalog_client.list_nodes_for_service(service_name)
        if live_nodes.ok?
          live_nodes.map(&:service_id)
        else
          raise "Unable to read from Consul catalog: #{live_nodes.content}"
        end
      end

      private

      def check_attributes
        {
          script: check_script,
          status: 'passing',
          interval: @config.fetch(:check_interval, '5m'),
          deregister_critical_service_after: @config.fetch(:deregister_service_delay, '10m'),
        }.tap do |h|
          h[:docker_container_id] = docker_container_id if @config['docker']
        end
      end

      def check_script
        return @check_script if @check_script
        stat = File::Stat.new("/proc/#{Process.pid}")
        @check_script = sprintf(SCRIPT_TEMPLATE, {pid: Process.pid, mtime: stat.mtime.to_i})
      end

      # This method is horrible, it takes advantage of the fact that docker uses
      # cgroups for part of its magic and also uses the container id as the cgroup name
      def docker_container_id
        return @docker_container_id if @docker_container_id
        content = File.read("/proc/1/cgroup").split("\n")
        @docker_container_id = content.last.split("/").last
      end

      def service_name
        @service_name ||= @config.fetch('service_name', DEFAULT_SERVICE_NAME)
      end
    end
  end
end
