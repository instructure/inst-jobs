# frozen_string_literal: true

class AddNStrandIndex < ActiveRecord::Migration[5.2]
  disable_ddl_transaction!

  def change
    add_index :delayed_jobs,
              %i[strand next_in_strand id],
              name: "n_strand_index",
              where: "strand IS NOT NULL",
              algorithm: :concurrently
  end
end
