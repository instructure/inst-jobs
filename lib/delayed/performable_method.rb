module Delayed
  class PerformableMethod < Struct.new(:object, :method, :args, :fail_cb, :permanent_fail_cb)
    def initialize(object, method, args = [], fail_cb = nil, permanent_fail_cb = nil)
      raise NoMethodError, "undefined method `#{method}' for #{object.inspect}" unless object.respond_to?(method, true)

      self.object = object
      self.args   = args
      self.method = method.to_sym
      self.fail_cb           = fail_cb
      self.permanent_fail_cb = permanent_fail_cb
    end

    def display_name
      if object.is_a?(Module)
        "#{object}.#{method}"
      else
        "#{object.class}##{method}"
      end
    end
    alias_method :tag, :display_name

    def perform
      object.send(method, *args)
    end

    def on_failure(error)
      object.send(fail_cb, error) if fail_cb
    end

    def on_permanent_failure(error)
      object.send(permanent_fail_cb, error) if permanent_fail_cb
    end

    def deep_de_ar_ize(arg)
      case arg
      when Hash
        "{#{arg.map { |k, v| "#{deep_de_ar_ize(k)} => #{deep_de_ar_ize(v)}" }.join(', ')}}"
      when Array
        "[#{arg.map { |a| deep_de_ar_ize(a) }.join(', ')}]"
      when ActiveRecord::Base
        "#{arg.class}.find(#{arg.id})"
      else
        arg.inspect
      end
    end

    def full_name
      obj_name = object.is_a?(ActiveRecord::Base) ? "#{object.class}.find(#{object.id}).#{method}" : display_name
      "#{obj_name}(#{args.map { |a| deep_de_ar_ize(a) }.join(', ')})"
    end
  end
end
