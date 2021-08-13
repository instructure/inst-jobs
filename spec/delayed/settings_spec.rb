# frozen_string_literal: true

require "spec_helper"

RSpec.describe Delayed::Settings do
  let(:configfile) do
    <<~YAML
      default:
        workers:
        - queue: myqueue
          workers: 2
        - queue: secondqueue
          max_priority: 7
        max_attempts: 1
    YAML
  end

  describe ".worker_config" do
    it "merges each worker config with the top-level config" do
      expect(File).to receive(:read).with("fname").and_return(configfile)
      config = described_class.worker_config("fname")
      expect(config[:workers]).to eq([
                                       { "queue" => "myqueue", "workers" => 2, "max_attempts" => 1 },
                                       { "queue" => "secondqueue", "max_priority" => 7, "max_attempts" => 1 }
                                     ])
    end
  end

  describe ".apply_worker_config!" do
    it "applies global settings from the given config" do
      expect(described_class).to receive(:last_ditch_logfile=).with(true)
      described_class.apply_worker_config!("last_ditch_logfile" => true)
    end

    it "merges in parent_process overrides to default config" do
      described_class.apply_worker_config!("parent_process" => { "foo" => "bar" })

      expect(Delayed::Settings.parent_process).to include("foo" => "bar")
    end
  end

  describe ".parent_process_client_timeout=" do
    it "must update the value in the parent_process settings hash" do
      Delayed::Settings.parent_process_client_timeout = 42
      expect(Delayed::Settings.parent_process["server_socket_timeout"]).to eq 42
    end
  end
end
