# frozen_string_literal: true

require "spec_helper"

RSpec.describe Delayed::CLI do
  describe "#parse_cli_options!" do
    it "correctly parses the --config option" do
      cli = described_class.new(%w[run --config /path/to/some/file.yml])
      options = cli.parse_cli_options!
      expect(options).to include config_file: "/path/to/some/file.yml"
    end
  end

  describe "#run" do
    before do
      expect(Delayed::Settings).to receive(:worker_config).and_return({})
    end

    it "prints help when no command is given" do
      cli = described_class.new([])
      expect(cli).to receive(:puts).with(/Usage/)
      cli.run
    end
  end
end
