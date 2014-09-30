class AddStoryTable < ActiveRecord::Migration
  def change
    create_table :stories do |table|
      table.string :text
    end
  end
end
