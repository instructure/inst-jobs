# frozen_string_literal: true

$:.push File.expand_path("lib", __dir__)

# Maintain your gem's version:
require "delayed/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "inst-jobs"
  s.version     = Delayed::VERSION
  s.authors     = ["Cody Cutrer", "Ethan Vizitei", "Jacob Burroughs"]
  s.email       = ["cody@instructure.com", "evizitei@instructure.com", "jburroughs@instructure.com"]
  s.homepage    = "https://github.com/instructure/inst-jobs"
  s.summary     = "Instructure-maintained fork of delayed_job"

  s.bindir = "exe"
  s.executables = ["inst_jobs"]
  s.files = Dir["{app,config,db,lib}/**/*"]

  s.metadata["rubygems_mfa_required"] = "true"

  s.required_ruby_version = ">= 3.2"

  s.add_dependency "activerecord",               ">= 7.1"
  s.add_dependency "activerecord-pg-extensions", "~> 0.4"
  s.add_dependency "activesupport",              ">= 7.1"
  s.add_dependency "after_transaction_commit",   ">= 1.0", "<3"
  s.add_dependency "debug_inspector",            "~> 1.0"
  s.add_dependency "fugit",                      "~> 1.3"
  s.add_dependency "railties",                   ">= 6.0"
end
