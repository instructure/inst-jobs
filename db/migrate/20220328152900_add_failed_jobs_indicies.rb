# frozen_string_literal: true

class AddFailedJobsIndicies < ActiveRecord::Migration[5.2]
  disable_ddl_transaction!

  def change
    add_index :failed_jobs, :failed_at, algorithm: :concurrently
    add_index :failed_jobs, :strand, where: "strand IS NOT NULL", algorithm: :concurrently
    add_index :failed_jobs, :singleton, where: "singleton IS NOT NULL", algorithm: :concurrently
    add_index :failed_jobs, :tag, algorithm: :concurrently
  end
end
