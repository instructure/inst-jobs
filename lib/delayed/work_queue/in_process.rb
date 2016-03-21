module Delayed
module WorkQueue
# The simplest possible implementation of a WorkQueue -- just turns around and
# queries the queue inline.
class InProcess
  def get_and_lock_next_available(worker_name, worker_config)
    Delayed::Worker.lifecycle.run_callbacks(:work_queue_pop, self, worker_name, worker_config) do
      Delayed::Job.get_and_lock_next_available(
        worker_name,
        worker_config[:queue],
        worker_config[:min_priority],
        worker_config[:max_priority])
    end
  end
end
end
end
