# frozen_string_literal: true

module Delayed
  class InvalidCallback < RuntimeError; end

  class Lifecycle
    EVENTS = {
      create: [:args],
      error: %i[worker job exception],
      exceptional_exit: %i[worker exception],
      execute: [:worker],
      invoke_job: [:job],
      loop: [:worker],
      perform: %i[worker job],
      pop: [:worker],
      retry: %i[worker job exception],
      work_queue_pop: %i[work_queue worker_config],
      check_for_work: [:work_queue]
    }.freeze

    def initialize
      reset!
    end

    def reset!
      @callbacks = EVENTS.keys.each_with_object({}) do |e, hash|
        hash[e] = Callback.new
        hash
      end
      Delayed::Worker.plugins.each(&:reset!)
    end

    def before(event, &)
      add(:before, event, &)
    end

    def after(event, &)
      add(:after, event, &)
    end

    def around(event, &)
      add(:around, event, &)
    end

    def run_callbacks(event, *args, &)
      missing_callback(event) unless @callbacks.key?(event)

      unless EVENTS[event].size == args.size
        raise ArgumentError, "Callback #{event} expects #{EVENTS[event].size} parameter(s): #{EVENTS[event].join(", ")}"
      end

      @callbacks[event].execute(*args, &)
    end

    private

    def add(type, event, &)
      missing_callback(event) unless @callbacks.key?(event)

      @callbacks[event].add(type, &)
    end

    def missing_callback(event)
      raise InvalidCallback, "Unknown callback event: #{event}"
    end
  end

  class Callback
    def initialize
      @before = []
      @after = []

      # Identity proc. Avoids special cases when there is no existing around chain.
      @around = ->(*args, &block) { block.call(*args) }
    end

    def execute(*args, &)
      @before.each { |c| c.call(*args) }
      result = @around.call(*args, &)
      @after.each do |c|
        if c.arity == args.length + 1 # don't fail for methods that don't define `result:`
          c.call(*args, result:)
        else
          c.call(*args)
        end
      end
      result
    end

    def add(type, &callback)
      case type
      when :before
        @before << callback
      when :after
        @after << callback
      when :around
        chain = @around # use a local variable so that the current chain is closed over in the following lambda
        @around = ->(*a, &block) { chain.call(*a) { |*b| callback.call(*b, &block) } }
      else
        raise InvalidCallback, "Invalid callback type: #{type}"
      end
    end
  end
end
