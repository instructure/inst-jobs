require 'spec_helper'

RSpec.describe Delayed::Settings do
  let(:configfile) {<<-YAML
default:
  workers:
  - queue: myqueue
    workers: 2
  - queue: secondqueue
    max_priority: 7
  max_attempts: 1
    YAML
  }

  describe '.worker_config' do
    it 'merges each worker config with the top-level config' do
      expect(File).to receive(:read).with("fname").and_return(configfile)
      config = described_class.worker_config("fname")
      expect(config[:workers]).to eq([
        {'queue' => 'myqueue', 'workers' => 2, 'max_attempts' => 1},
        {'queue' => 'secondqueue', 'max_priority' => 7, 'max_attempts' => 1},
      ])
    end
  end

  describe '.apply_worker_config!' do
    it 'applies global settings from the given config' do
      expect(described_class).to receive(:last_ditch_logfile=).with(true)
      described_class.apply_worker_config!('last_ditch_logfile' => true)
    end
  end
end
