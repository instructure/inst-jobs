# frozen_string_literal: true

$:.push File.expand_path("lib", __dir__)

# Maintain your gem's version:
require "delayed/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "inst-jobs"
  s.version     = Delayed::VERSION
  s.authors     = ["Tobias Luetke", "Brian Palmer"]
  s.email       = ["brianp@instructure.com"]
  s.homepage    = "https://github.com/instructure/inst-jobs"
  s.summary     = "Instructure-maintained fork of delayed_job"

  s.bindir = "exe"
  s.executables = ["inst_jobs"]
  s.files = Dir["{app,config,db,lib}/**/*"]
  s.test_files = Dir["spec/**/*"]

  s.required_ruby_version = ">= 2.6"

  s.add_dependency "activerecord",             ">= 5.2"
  s.add_dependency "activesupport",            ">= 5.2"
  s.add_dependency "after_transaction_commit", ">= 1.0", "<3"
  s.add_dependency "debug_inspector",          "~> 1.0"
  s.add_dependency "fugit",                    "~> 1.3"
  s.add_dependency "railties",                 ">= 5.2"

  s.add_development_dependency "bump"
  s.add_development_dependency "byebug"
  s.add_development_dependency "database_cleaner", "~> 2.0"
  s.add_development_dependency "database_cleaner-active_record", "~> 2.0"
  s.add_development_dependency "diplomat", "~> 2.5.1"
  s.add_development_dependency "pg"
  s.add_development_dependency "pry"
  s.add_development_dependency "rack-test"
  s.add_development_dependency "rails"
  s.add_development_dependency "rake"
  s.add_development_dependency "rspec", "~> 3.10"
  s.add_development_dependency "rubocop", "~> 1.19"
  s.add_development_dependency "sinatra"
  s.add_development_dependency "sinatra-contrib"
  s.add_development_dependency "timecop", "0.9.4"
  s.add_development_dependency "webmock"
  s.add_development_dependency "wwtd", "~> 1.4.0"
end
