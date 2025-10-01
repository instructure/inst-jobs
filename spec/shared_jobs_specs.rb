# frozen_string_literal: true

require_relative "shared/delayed_batch"
require_relative "shared/delayed_method"
require_relative "shared/performable_method"
require_relative "shared/shared_backend"
require_relative "shared/testing"
require_relative "shared/worker"

shared_examples_for "a delayed_jobs implementation" do
  it_behaves_like "a backend"
  it_behaves_like "Delayed::Batch"
  it_behaves_like "random ruby objects"
  it_behaves_like "Delayed::PerformableMethod"
  it_behaves_like "Delayed::Worker"
  it_behaves_like "Delayed::Testing"
end
