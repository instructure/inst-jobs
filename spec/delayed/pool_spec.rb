require 'spec_helper'

RSpec.describe Delayed::Pool do
  describe '#parse_cli_options!' do
    it 'must correctly parse the --config option' do
      pool = Delayed::Pool.new(%w{run --config /path/to/some/file.yml})
      pool.parse_cli_options!
      expect(pool.options).to include config_file: '/path/to/some/file.yml'
    end
  end
end
