# frozen_string_literal: true

require "debug_inspector" if ::Rails.env.test? || ::Rails.env.development?

module Delayed
  module MessageSending
    class DelayProxy < BasicObject
      def initialize(object, synchronous: false, sender: nil, **enqueue_args)
        @object = object
        @enqueue_args = enqueue_args
        @synchronous = synchronous
        @sender = sender
      end

      def method_missing(method, *args, **kwargs) # rubocop:disable Style/MissingRespondToMissing
        # method doesn't exist? must be method_missing; assume private access
        @sender = nil if !@sender.nil? &&
                         @object.methods.exclude?(method) &&
                         @object.protected_methods.exclude?(method) &&
                         @object.private_methods.exclude?(method)

        sender_is_object = @sender == @object
        sender_is_class = @sender.is_a?(@object.class)

        # even if the call is async, if the call is _going_ to generate an error, we make it synchronous
        # so that the error is generated immediately, instead of waiting for it to fail in a job,
        # which might go unnoticed
        if !@sender.nil? && !@synchronous
          @synchronous = true if !sender_is_object && @object.private_methods.include?(method)
          @synchronous = true if !sender_is_class && @object.protected_methods.include?(method)
        end

        if @synchronous
          if @sender.nil? || sender_is_object || (sender_is_class && @object.protected_methods.include?(method))
            return @object.send(method, *args) if kwargs.empty?

            return @object.send(method, *args, **kwargs)
          end

          return @object.public_send(method, *args) if kwargs.empty?

          return @object.public_send(method, *args, **kwargs)
        end

        ignore_transaction = @enqueue_args.delete(:ignore_transaction)
        on_failure = @enqueue_args.delete(:on_failure)
        on_permanent_failure = @enqueue_args.delete(:on_permanent_failure)
        unless ignore_transaction
          # delay queuing up the job in another database until the results of the current
          # transaction are visible
          connection = @object.class.connection if @object.class.respond_to?(:connection)
          connection ||= @object.connection if @object.respond_to?(:connection)
          connection ||= ::ActiveRecord::Base.connection

          if ::Delayed::Job != ::Delayed::Backend::ActiveRecord::Job || connection != ::Delayed::Job.connection
            connection.after_transaction_commit do
              ::Delayed::Job.enqueue(::Delayed::PerformableMethod.new(@object, method,
                                                                      args: args, kwargs: kwargs,
                                                                      on_failure: on_failure,
                                                                      on_permanent_failure: on_permanent_failure,
                                                                      sender: @sender),
                                     **@enqueue_args)
            end
            return nil
          end
        end

        result = ::Delayed::Job.enqueue(::Delayed::PerformableMethod.new(@object, method,
                                                                         args: args,
                                                                         kwargs: kwargs,
                                                                         on_failure: on_failure,
                                                                         on_permanent_failure: on_permanent_failure,
                                                                         sender: @sender),
                                        **@enqueue_args)
        result = nil unless ignore_transaction
        result
      end
    end

    def delay(sender: nil, **enqueue_args)
      # support procs/methods as enqueue arguments
      enqueue_args.each do |k, v|
        enqueue_args[k] = v.call(self) if v.respond_to?(:call)
      end

      sender ||= __calculate_sender_for_delay

      DelayProxy.new(self, sender: sender, **enqueue_args)
    end

    def __calculate_sender_for_delay
      # enforce public send in dev and test, but not prod (since it uses
      # debug APIs, it's expensive)
      return sender(1) if ::Rails.env.test? || ::Rails.env.development?
    end

    module ClassMethods
      KWARG_ARG_TYPES = %i[key keyreq keyrest].freeze
      private_constant :KWARG_ARG_TYPES

      def handle_asynchronously(method_name, **enqueue_args)
        visibility = if public_method_defined?(method_name)
                       :public
                     elsif private_method_defined?(method_name)
                       :private
                     else
                       :protected
                     end

        if kwargs? method_name
          generated_delayed_methods.class_eval do
            define_method(method_name, lambda do |*args, synchronous: false, **kwargs|
              if synchronous
                super(*args, **kwargs)
              else
                delay(sender: __calculate_sender_for_delay, **enqueue_args)
                  .method_missing(method_name, *args, synchronous: true, **kwargs)
              end
            end)
          end
        else
          generated_delayed_methods.class_eval do
            define_method(method_name, lambda do |*args, synchronous: false|
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

      def kwargs?(method_name)
        original_arg_types = instance_method(method_name).parameters.map(&:first)
        original_arg_types.any? { |arg_type| KWARG_ARG_TYPES.include?(arg_type) }
      end
    end
  end
end
