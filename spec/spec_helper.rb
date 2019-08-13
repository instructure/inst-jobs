require 'delayed_job'
require 'delayed/testing'

require 'database_cleaner'
require 'rack/test'
require 'test_after_commit' if ::Rails.version < '5'
require 'timecop'
require 'pry'
require 'byebug'

RSpec.configure do |config|

  config.expect_with(:rspec) do |c|
    c.syntax = [:should, :expect]
  end

  config.before(:suite) do
    DatabaseCleaner.strategy = :transaction
    DatabaseCleaner.clean_with(:truncation)
  end

  config.before(:each) do |example|
    if Delayed::Backend::Redis::Job.redis
      Delayed::Backend::Redis::Job.redis.flushdb
    end
    DatabaseCleaner.strategy = (example.metadata[:sinatra] || example.metadata[:non_transactional]) ?
        :truncation : :transaction
    DatabaseCleaner.start
  end

  config.after(:each) do
    DatabaseCleaner.clean
  end
end

ENV['TEST_ENV_NUMBER'] ||= '1'
ENV['TEST_DB_HOST'] ||= 'localhost'
ENV['TEST_DB_DATABASE'] ||= "inst-jobs-test-#{ENV['TEST_ENV_NUMBER']}"
ENV['TEST_REDIS_CONNECTION'] ||= 'redis://localhost:6379/'

Delayed::Backend::Redis::Job.redis = Redis.new(url: ENV['TEST_REDIS_CONNECTION'])
Delayed::Backend::Redis::Job.redis.select ENV['TEST_ENV_NUMBER']

connection_config = {
  adapter: :postgresql,
  host: ENV['TEST_DB_HOST'].presence,
  encoding: 'utf8',
  username: ENV['TEST_DB_USERNAME'],
  database: ENV['TEST_DB_DATABASE'],
}

if ::Rails.version < '5'
  class ActiveRecord::Migration
    class << self
      def [](_version); self; end
    end
  end
end

def migrate(file)
  if ::Rails.version < '5.2'
    ActiveRecord::Migrator.migrate(file)
  elsif ::Rails.version >= '6'
    ActiveRecord::MigrationContext.new(file, ActiveRecord::SchemaMigration).migrate
  else
    ActiveRecord::MigrationContext.new(file).migrate
  end
end

# create the test db if it does not exist, to help out wwtd
ActiveRecord::Base.establish_connection(connection_config.merge(database: 'postgres'))
begin
  ActiveRecord::Base.connection.create_database(connection_config[:database])
rescue ActiveRecord::StatementInvalid
end
ActiveRecord::Base.establish_connection(connection_config)
# TODO reset db and migrate again, to test migrations

migrate("db/migrate")
migrate("spec/migrate")
Delayed::Backend::ActiveRecord::Job.reset_column_information
Delayed::Backend::ActiveRecord::Job::Failed.reset_column_information

Time.zone = 'UTC'
Rails.logger = Logger.new(nil)

# Purely useful for test cases...
class Story < ActiveRecord::Base
  def tell; text; end
  def whatever(n, _); tell*n; end
  def whatever_else(n, _); tell*n; end

  handle_asynchronously :whatever
  handle_asynchronously_with_queue :whatever_else, "testqueue"
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
  old_val = klass.class_variable_get(:"@@#{setting_name}")
  klass.send("#{setting_name}=", value)
  yield
ensure
  klass.send("#{setting_name}=", old_val)
end

def run_job(job)
  Delayed::Testing.run_job(job)
end

require File.expand_path('../sample_jobs', __FILE__)
require File.expand_path('../shared_jobs_specs', __FILE__)
