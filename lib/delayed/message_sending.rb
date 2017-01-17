module Delayed
  module MessageSending
    def send_later(method, *args)
      send_later_enqueue_args(method, {}, *args)
    end

    def send_later_enqueue_args(method, enqueue_args = {}, *args)
      enqueue_args = enqueue_args.dup
      # support procs/methods as enqueue arguments
      enqueue_args.each do |k,v|
        if v.respond_to?(:call)
          enqueue_args[k] = v.call(self)
        end
      end

      no_delay = enqueue_args.delete(:no_delay)
      on_failure = enqueue_args.delete(:on_failure)
      on_permanent_failure = enqueue_args.delete(:on_permanent_failure)
      if !no_delay
        # delay queuing up the job in another database until the results of the current
        # transaction are visible
        connection = self.class.connection if self.class.respond_to?(:connection)
        connection ||= self.connection if respond_to?(:connection)
        connection ||= ActiveRecord::Base.connection

        if (Delayed::Job != Delayed::Backend::ActiveRecord::Job || connection != Delayed::Job.connection)
          connection.after_transaction_commit do
            Delayed::Job.enqueue(Delayed::PerformableMethod.new(self, method.to_sym, args,
                                                                on_failure, on_permanent_failure), enqueue_args)
          end
          return nil
        end
      end

      result = Delayed::Job.enqueue(Delayed::PerformableMethod.new(self, method.to_sym, args,
                                                                   on_failure, on_permanent_failure), enqueue_args)
      result = nil unless no_delay
      result
    end

    def send_later_with_queue(method, queue, *args)
      send_later_enqueue_args(method, { :queue => queue }, *args)
    end

    def send_at(time, method, *args)
      send_later_enqueue_args(method,
                          { :run_at => time }, *args)
    end

    def send_at_with_queue(time, method, queue, *args)
      send_later_enqueue_args(method,
                          { :run_at => time, :queue => queue },
                          *args)
    end

    def send_later_unless_in_job(method, *args)
      if Delayed::Job.in_delayed_job?
        send(method, *args)
      else
        send_later(method, *args)
      end
      nil # can't rely on the type of return value, so return nothing
    end

    def send_later_if_production(*args)
      if Rails.env.production?
        send_later(*args)
      else
        send(*args)
      end
    end

    def send_later_if_production_enqueue_args(method, enqueue_args, *args)
      if Rails.env.production?
        send_later_enqueue_args(method, enqueue_args, *args)
      else
        send(method, *args)
      end
    end

    def send_now_or_later(_when, *args)
      if _when == :now
        send(*args)
      else
        send_later(*args)
      end
    end

    def send_now_or_later_if_production(_when, *args)
      if _when == :now
        send(*args)
      else
        send_later_if_production(*args)
      end
    end

    module ClassMethods
      def add_send_later_methods(method, enqueue_args={}, default_async=false)
        aliased_method, punctuation = method.to_s.sub(/([?!=])$/, ''), $1

        # we still need this for backwards compatibility
        without_method = "#{aliased_method}_without_send_later#{punctuation}"

        if public_method_defined?(method)
          visibility = :public
        elsif private_method_defined?(method)
          visibility = :private
        else
          visibility = :protected
        end

        generated_delayed_methods.class_eval(<<-EOF, __FILE__, __LINE__ + 1)
          def #{without_method}(*args)
            send(:#{method}, *args, synchronous: true)
          end
          #{visibility} :#{without_method}

          def #{method}(*args, synchronous: #{!default_async})
            if synchronous
              super(*args)
            else
              send_later_enqueue_args(:#{method}, #{enqueue_args.inspect}, *args, synchronous: true)
            end
          end
          #{visibility} :#{method}
        EOF
      end

      def handle_asynchronously(method, enqueue_args={})
        add_send_later_methods(method, enqueue_args, true)
      end

      def handle_asynchronously_with_queue(method, queue)
        add_send_later_methods(method, {:queue => queue}, true)
      end

      def handle_asynchronously_if_production(method, enqueue_args={})
        add_send_later_methods(method, enqueue_args, Rails.env.production?)
      end

      private def generated_delayed_methods
        @generated_delayed_methods ||= Module.new.tap do |mod|
          const_set(:DelayedMethods, mod)
          prepend mod
        end
      end
    end
  end
end
