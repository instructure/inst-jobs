# frozen_string_literal: true

require "delayed/plugin"

module Delayed
  class RailsReloaderPlugin < Plugin
    callbacks do |lifecycle|
      app = Rails.application
      if app && !app.config.cache_classes
        lifecycle.around(:perform) do |worker, job, &block|
          reload = !app.config.reload_classes_only_on_change || app.reloaders.any?(&:updated?)

          if reload
            if defined?(ActiveSupport::Reloader)
              Rails.application.reloader.reload!
            else
              ActionDispatch::Reloader.prepare!
            end
          end

          begin
            block.call(worker, job)
          ensure
            ActionDispatch::Reloader.cleanup! if reload && !defined?(ActiveSupport::Reloader)
          end
        end
      end
    end
  end
end
