require 'delayed/daemon/task'

require 'thread'

module Delayed
class QueueProxy < Task
  attr_reader :queue, :min_priority, :max_priority

  @instance = nil
  @instance_mutex = Mutex.new

  def self.instance
    return @instance if @instance
    @instance_mutex.synchronize {
      return @instance if @instance
      @instance = new()
    }
    @instance
  end

  def initialize
    super
    @stats = {}
  end

  def stats_for_thread(thread)
    @stats[thread.object_id] ||= Hash.new(0)
  end

  def stats_for_operation(thread, op)
    stats_for_thread(thread)[op]
  end

  def run_as_thread
    @request_queue = Queue.new
    super
  end

  def execute
    raise(ArgumentError, "process fetcher not yet supported") if process?
    run_loop do
      if @request_queue.empty?
        sleep 0.1 # can't block forever, so we spinwait
      else
        op, request, responder = @request_queue.pop(true)
        increment(op)
        responder << request.()
      end
    end
  end

  def get_and_lock_next_available(worker_name, queue, min_priority, max_priority)
    queue_or_run(:get) do
      Delayed::Job.get_and_lock_next_available(
        worker_name,
        queue,
        min_priority,
        max_priority)
    end
  end

  def destroy_job(job)
    queue_or_run(:destroy) do
      job.destroy
    end
  end

  def create_and_lock!(job, worker_name)
    queue_or_run(:create_and_lock!) do
      job.create_and_lock!(worker_name)
    end
  end

  def reschedule(job, error)
    queue_or_run(:reschedule) do
      job.reschedule(error)
    end
  end

  private

  def queue_or_run(op, &cb)
    if @request_queue
      responder = Queue.new
      @request_queue << [op, cb, responder]
      responder.pop
    else
      increment(op)
      cb.()
    end
  end

  def increment(op)
    stats_for_thread(Thread.current)[op] += 1
  end
end
end
