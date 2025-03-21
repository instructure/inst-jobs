# frozen_string_literal: true

module Delayed
  class Engine < ::Rails::Engine
    initializer "delayed_job.set_reloader_hook" do |app|
      Delayed::Worker.lifecycle.around(:perform) do |worker, job, &block|
        kwargs = (Rails.version < "7.1") ? {} : { source: "application.delayed_job" }

        app.reloader.wrap(**kwargs) do
          block.call(worker, job)
        end
      end
    end
  end
end
