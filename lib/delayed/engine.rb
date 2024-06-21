# frozen_string_literal: true

module Delayed
  class Engine < ::Rails::Engine
    initializer "delayed_job.set_reloader_hook" do |app|
      Delayed::Worker.lifecycle.around(:perform) do |worker, job, &block|
        app.reloader.wrap(source: "application.delayed_job") do
          block.call(worker, job)
        end
      end
    end
  end
end
