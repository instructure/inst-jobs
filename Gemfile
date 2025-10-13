# frozen_string_literal: true

source "https://rubygems.org"

plugin "bundler-multilock", "1.3.4"
return unless Plugin.installed?("bundler-multilock")

Plugin.send(:load_plugin, "bundler-multilock")

gemspec

lockfile "activerecord-7.1" do
  gem "activerecord", "~> 7.1.0"
  gem "railties", "~> 7.1.0"
end

lockfile "activerecord-7.2" do
  gem "activerecord", "~> 7.2.0"
  gem "railties", "~> 7.2.0"
end

lockfile do
  gem "activerecord", "~> 8.0.0"
  gem "railties", "~> 8.0.0"
end

group :development, :test do
  gem "bump"
  gem "database_cleaner", "~> 2.0"
  gem "database_cleaner-active_record", "~> 2.0"
  gem "debug"
  gem "diplomat", "~> 2.6.3"
  gem "mutex_m"
  gem "pg"
  gem "rack-test"
  gem "rake"
  gem "rspec", "~> 3.10"
  gem "rubocop-inst", "~> 1"
  gem "rubocop-rails", "~> 2.11"
  gem "rubocop-rake", "~> 0.6"
  gem "rubocop-rspec", "~> 3.0"
  gem "sinatra", ">= 4.2.0"
  gem "sinatra-contrib", "~> 4.2", ">= 4.2.0"
  gem "timecop", "~> 0.9"
  gem "webmock"
end
