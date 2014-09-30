require 'delayed_job'

require 'database_cleaner'
require 'test_after_commit'
require 'timecop'
require 'pry'

RSpec.configure do |config|

  config.expect_with(:rspec) do |c|
    c.syntax = [:should, :expect]
  end

  config.before(:suite) do
    DatabaseCleaner.strategy = :transaction
    DatabaseCleaner.clean_with(:truncation)
  end

  config.around(:each) do |example|
    if Delayed::Backend::Redis::Job.redis
      Delayed::Backend::Redis::Job.redis.flushdb
    end
    DatabaseCleaner.cleaning do
      example.run
    end
  end

end

ENV['TEST_ENV_NUMBER'] ||= '1'
ENV['TEST_DB_HOST'] ||= 'localhost'
ENV['TEST_DB_DATABASE'] ||= "canvas-jobs-test-#{ENV['TEST_ENV_NUMBER']}"
ENV['TEST_REDIS_CONNECTION'] ||= 'redis://localhost:6379/'

Delayed::Backend::Redis::Job.redis = Redis.new(url: ENV['TEST_REDIS_CONNECTION'])

ActiveRecord::Base.establish_connection({
  adapter: :postgresql,
  host: ENV['TEST_DB_HOST'],
  encoding: 'utf8',
  username: ENV['TEST_DB_USERNAME'],
  database: ENV['TEST_DB_DATABASE'],
})
# TODO reset db and migrate again, to test migrations

ActiveRecord::Migrator.migrate("db/migrate")
ActiveRecord::Migrator.migrate("spec/migrate")

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
  Delayed::Worker.new.perform(job)
end

require File.expand_path('../sample_jobs', __FILE__)
require File.expand_path('../shared_jobs_specs', __FILE__)
