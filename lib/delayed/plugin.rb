require 'active_support/core_ext/class/attribute'

module Delayed
  class Plugin
    class_attribute :callback_block

    def self.callbacks(&block)
      self.callback_block = block
    end

    def self.inject!
      unless @injected
        self.callback_block.call(Delayed::Worker.lifecycle) if self.callback_block
      end
      @injected = true
    end

    def self.reset!
      @injected = false
    end
  end
end
