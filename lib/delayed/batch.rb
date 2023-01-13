# frozen_string_literal: true

module Delayed
  module Batch
    PerformableBatch = Struct.new(:mode, :items) do
      def initialize(mode, items)
        raise "unsupported mode" unless mode == :serial

        super
      end

      def display_name
        "Delayed::Batch.#{mode}"
      end
      alias_method :tag, :display_name
      alias_method :full_name, :display_name

      def perform
        raise "can't perform a batch directly"
      end

      def jobs
        items.map { |opts| Delayed::Job.new(**opts) }
      end
    end

    class << self
      def serial_batch(opts = {}, &block)
        prepare_batches(:serial, opts, &block)
      end

      private

      def prepare_batches(mode, opts)
        raise "nested batching is not supported" if Delayed::Job.batches

        Delayed::Job.batches = Hash.new { |h, k| h[k] = Set.new }
        batch_enqueue_args = [:queue]
        batch_enqueue_args << :priority unless opts[:priority]
        Delayed::Job.batch_enqueue_args = batch_enqueue_args
        yield
      ensure
        batches = Delayed::Job.batches
        Delayed::Job.batches = nil
        batch_args = opts.slice(:priority)
        batches.each do |enqueue_args, batch|
          next if batch.empty?

          if batch.size == 1
            args = batch.first.merge(batch_args)
            payload_object = args.delete(:payload_object)
            Delayed::Job.enqueue(payload_object, **args)
          else
            Delayed::Job.enqueue(Delayed::Batch::PerformableBatch.new(mode, batch), **enqueue_args.merge(batch_args))
          end
        end
      end
    end
  end
end
