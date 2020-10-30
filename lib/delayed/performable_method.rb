# frozen_string_literal: true

module Delayed
  class PerformableMethod < Struct.new(:object, :method, :args, :kwargs, :fail_cb, :permanent_fail_cb, :sender)
    def initialize(object, method, args: [], kwargs: {}, on_failure: nil, on_permanent_failure: nil, sender: nil)
      raise NoMethodError, "undefined method `#{method}' for #{object.inspect}" unless object.respond_to?(method, true)

      self.object = object
      self.args   = args
      self.kwargs = kwargs
      self.method = method.to_sym
      self.fail_cb           = on_failure
      self.permanent_fail_cb = on_permanent_failure
      self.sender = sender
      begin
        YAML.load(YAML.dump(sender))
      rescue
        # if for some reason you can't dump the sender, just drop it
        self.sender = nil
      end
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
      kwargs = self.kwargs || {}

      sender_is_object = sender == object
      sender_is_class = sender.is_a?(object.class)

      if sender.nil? || sender_is_object || sender_is_class && object.protected_methods.include?(method)
        if kwargs.empty?
          object.send(method, *args)
        else
          object.send(method, *args, **kwargs)
        end
      else
        if kwargs.empty?
          object.public_send(method, *args)
        else
          object.public_send(method, *args, **kwargs)
        end
      end
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
      kwargs_str = kwargs.map { |(k, v)| ", #{k}: #{deep_de_ar_ize(v)}"}.join("")
      "#{obj_name}(#{args.map { |a| deep_de_ar_ize(a) }.join(', ')}#{kwargs_str})"
    end
  end
end
