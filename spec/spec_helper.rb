# frozen_string_literal: true

require "delayed_job"
require "delayed/testing"

require "database_cleaner"
require "fileutils"
require "rack/test"
require "timecop"
require "webmock/rspec"

require "debug/prelude"

RSpec.configure do |config|
  config.expect_with(:rspec) do |c|
    c.syntax = %i[should expect]
  end

  config.before(:suite) do
    DatabaseCleaner.strategy = :transaction
    DatabaseCleaner.clean_with(:truncation)
    WebMock.disable_net_connect!
  end

  config.before do |example|
    DatabaseCleaner.strategy = if example.metadata[:sinatra] || example.metadata[:non_transactional]
                                 :truncation
                               else
                                 :transaction
                               end
    DatabaseCleaner.start
  end

  config.after do
    DatabaseCleaner.clean
  end
end

module NoYamlDump
  def encode_with(coder); end
end
# example groups are often the sender, and if we try to serialize them,
# the resultant object is then encoded in the sender, and then we serialize
# again, and it just keeps getting bigger and bigger and bigger...
RSpec::Core::ExampleGroup.include(NoYamlDump)

ENV["TEST_ENV_NUMBER"] ||= "1"
ENV["TEST_DB_HOST"] ||= "localhost"
ENV["TEST_DB_DATABASE"] ||= "inst-jobs-test-#{ENV.fetch("TEST_ENV_NUMBER", nil)}"

connection_config = {
  adapter: :postgresql,
  host: ENV["TEST_DB_HOST"].presence,
  encoding: "utf8",
  username: ENV.fetch("TEST_DB_USERNAME", nil),
  database: ENV.fetch("TEST_DB_DATABASE", nil),
  min_messages: "notice",
  # Ensure the pool is big enough the deadlock tests don't get starved for connections by rails instead
  pool: 20
}

def migrate(file)
  ActiveRecord::MigrationContext.new(file, ActiveRecord::SchemaMigration).migrate
end

# create the test db if it does not exist
ActiveRecord::Base.establish_connection(connection_config.merge(database: "postgres"))
begin
  ActiveRecord::Base.connection.create_database(connection_config[:database])
rescue ActiveRecord::StatementInvalid
  nil
end
ActiveRecord::Base.establish_connection(connection_config)

# we need to ensure this callback is called for activerecord-pg-extensions,
# which isn't running because we're not using Rails to setup the database
ActiveRecord::PGExtensions::Railtie.run_initializers

# TODO: reset db and migrate again, to test migrations

migrate("db/migrate")
migrate("spec/migrate")
Delayed::Backend::ActiveRecord::Job.reset_column_information
Delayed::Backend::ActiveRecord::Job::Failed.reset_column_information

Time.zone = "UTC" # rubocop:disable Rails/TimeZoneAssignment
FileUtils.mkdir_p("tmp")
ActiveRecord::Base.logger = Rails.logger = Logger.new("tmp/test.log")

# Purely useful for test cases...
class Story < ActiveRecord::Base
  def tell
    text
  end

  def whatever(times, _)
    tell * times
  end

  def whatever_else(times, _)
    tell * times
  end

  handle_asynchronously :whatever
  handle_asynchronously :whatever_else, queue: "testqueue"
end

class StoryReader
  def read(story)
    "Epilog: #{story.tell}"
  end

  def self.reverse(str)
    str.reverse
  end
end

module MyReverser
  def self.reverse(str)
    str.reverse
  end
end

def change_setting(klass, setting_name, value)
  old_val = klass.send(setting_name)
  klass.send("#{setting_name}=", value)
  yield
ensure
  klass.send("#{setting_name}=", old_val)
end

def run_job(job)
  Delayed::Testing.run_job(job)
end

require_relative "sample_jobs"
require_relative "shared_jobs_specs"
