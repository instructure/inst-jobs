# frozen_string_literal: true

class AddStoryTable < ActiveRecord::Migration[4.2]
  def change
    create_table :stories do |table|
      table.string :text
    end
  end
end
