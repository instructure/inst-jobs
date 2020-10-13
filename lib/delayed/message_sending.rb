module Delayed
  module MessageSending
    class DelayProxy < BasicObject
      def initialize(object, enqueue_args)
        @object = object
        @enqueue_args = enqueue_args
      end

      def method_missing(method, *args, **kwargs)
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

    def delay(**enqueue_args)
      # support procs/methods as enqueue arguments
      enqueue_args.each do |k,v|
        if v.respond_to?(:call)
          enqueue_args[k] = v.call(self)
        end
      end
      if enqueue_args.delete(:synchronous)
        return self
      end
      DelayProxy.new(self, enqueue_args)
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
