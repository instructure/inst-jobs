# frozen_string_literal: true

if ::Rails.env.test? || ::Rails.env.development?
  require 'debug_inspector'
end

module Delayed
  module MessageSending
    class DelayProxy < BasicObject
      def initialize(object, synchronous: false, public_send: false, **enqueue_args)
        @object = object
        @enqueue_args = enqueue_args
        @synchronous = synchronous
        @public_send = public_send
      end

      def method_missing(method, *args, **kwargs)
        if @synchronous
          if @public_send
            if kwargs.empty?
              return @object.public_send(method, *args)
            else
              return @object.public_send(method, *args, **kwargs)
            end
          else
            if kwargs.empty?
              return @object.send(method, *args)
            else
              return @object.send(method, *args, **kwargs)
            end
          end
        end

        if @public_send && @object.private_methods.include?(method)
          ::Kernel.raise ::NoMethodError.new("undefined method `#{method}' for #{@object}", method)
        end

        ignore_transaction = @enqueue_args.delete(:ignore_transaction)
        on_failure = @enqueue_args.delete(:on_failure)
        on_permanent_failure = @enqueue_args.delete(:on_permanent_failure)
        if !ignore_transaction
          # delay queuing up the job in another database until the results of the current
          # transaction are visible
          connection = @object.class.connection if @object.class.respond_to?(:connection)
          connection ||= @object.connection if @object.respond_to?(:connection)
          connection ||= ::ActiveRecord::Base.connection

          if (::Delayed::Job != ::Delayed::Backend::ActiveRecord::Job || connection != ::Delayed::Job.connection)
            connection.after_transaction_commit do
              ::Delayed::Job.enqueue(::Delayed::PerformableMethod.new(@object, method,
                                                                  args: args, kwargs: kwargs,
                                                                  on_failure: on_failure,
                                                                  on_permanent_failure: on_permanent_failure),
                                                                  **@enqueue_args)
            end
            return nil
          end
        end

        result = ::Delayed::Job.enqueue(::Delayed::PerformableMethod.new(@object, method,
                                                                    args: args,
                                                                    kwargs: kwargs,
                                                                    on_failure: on_failure,
                                                                    on_permanent_failure: on_permanent_failure),
                                                                    **@enqueue_args)
        result = nil unless ignore_transaction
        result
      end
    end

    def delay(public_send: nil, **enqueue_args)
      # support procs/methods as enqueue arguments
      enqueue_args.each do |k,v|
        if v.respond_to?(:call)
          enqueue_args[k] = v.call(self)
        end
      end

      public_send ||= __calculate_public_send_for_delay

      DelayProxy.new(self, public_send: public_send, **enqueue_args)
    end

    def __calculate_public_send_for_delay
      # enforce public send in dev and test, but not prod (since it uses
      # debug APIs, it's expensive)
      public_send = if ::Rails.env.test? || ::Rails.env.development?
        sender = self.sender(1)
        # if the caller isn't self, use public_send; i.e. enforce method visibility
        sender != self
      else
        false
      end
    end

    module ClassMethods
      KWARG_ARG_TYPES = %i{key keyreq keyrest}.freeze
      private_constant :KWARG_ARG_TYPES

      def handle_asynchronously(method_name, **enqueue_args)
        aliased_method, punctuation = method_name.to_s.sub(/([?!=])$/, ''), $1

        if public_method_defined?(method_name)
          visibility = :public
        elsif private_method_defined?(method_name)
          visibility = :private
        else
          visibility = :protected
        end

        if has_kwargs? method_name
          generated_delayed_methods.class_eval do
            define_method(method_name, -> (*args, synchronous: false, **kwargs) do
              if synchronous
                super(*args, **kwargs)
              else
                delay(**enqueue_args).method_missing(method_name, *args, synchronous: true, **kwargs)
              end
            end)
          end
        else
          generated_delayed_methods.class_eval do
            define_method(method_name, -> (*args, synchronous: false) do
              if synchronous
                super(*args)
              else
                delay(**enqueue_args).method_missing(method_name, *args, synchronous: true)
              end
            end)
          end
        end
        generated_delayed_methods.send(visibility, method_name)
      end

      private

      def generated_delayed_methods
        @generated_delayed_methods ||= Module.new.tap do |mod|
          const_set(:DelayedMethods, mod)
          prepend mod
        end
      end

      def has_kwargs?(method_name)
        original_arg_types = instance_method(method_name).parameters.map(&:first)
        original_arg_types.any? { |arg_type| KWARG_ARG_TYPES.include?(arg_type) }
      end
    end
  end
end
