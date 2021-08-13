# frozen_string_literal: true

require_relative "shared/delayed_batch"
require_relative "shared/delayed_method"
require_relative "shared/performable_method"
require_relative "shared/shared_backend"
require_relative "shared/testing"
require_relative "shared/worker"

shared_examples_for "a delayed_jobs implementation" do
  include_examples "a backend"
  include_examples "Delayed::Batch"
  include_examples "random ruby objects"
  include_examples "Delayed::PerformableMethod"
  include_examples "Delayed::Worker"
  include_examples "Delayed::Testing"
end
