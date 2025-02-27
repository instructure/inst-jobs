# frozen_string_literal: true

module ActiveRecord
  class Base
    def self.load_for_delayed_job(id)
      if id
        find(id)
      else
        super
      end
    end
  end
end

module Delayed
  module Backend
    module ActiveRecord
      class AbstractJob < ::ActiveRecord::Base
        self.abstract_class = true
      end

      # A job object that is persisted to the database.
      # Contains the work object as a YAML field.
      class Job < AbstractJob
        include Delayed::Backend::Base
        self.table_name = :delayed_jobs

        attr_accessor :enqueue_result

        scope :next_in_strand_order, -> { order(:strand_order_override, :id) }

        def self.reconnect!
          ::ActiveRecord::Base.connection_handler.clear_all_connections!(nil)
        end

        class << self
          def create(attributes, &block)
            on_conflict = attributes.delete(:on_conflict)
            # modified from ActiveRecord::Persistence.create and ActiveRecord::Persistence#_insert_record
            job = new(attributes, &block)
            job.single_step_create(on_conflict: on_conflict)
          end

          def attempt_advisory_lock(lock_name)
            fn_name = connection.quote_table_name("half_md5_as_bigint")
            lock_name = connection.quote_string(lock_name)
            connection.select_value("SELECT pg_try_advisory_xact_lock(#{fn_name}('#{lock_name}'));")
          end

          def advisory_lock(lock_name)
            fn_name = connection.quote_table_name("half_md5_as_bigint")
            lock_name = connection.quote_string(lock_name)
            connection.execute("SELECT pg_advisory_xact_lock(#{fn_name}('#{lock_name}'));")
          end
        end

        def single_step_create(on_conflict: nil)
          connection = self.class.connection

          # a before_save callback that we're skipping
          initialize_defaults

          current_time = current_time_from_proper_timezone

          all_timestamp_attributes_in_model.each do |column|
            _write_attribute(column, current_time) unless attribute_present?(column)
          end

          attribute_names = attribute_names_for_partial_inserts
          attribute_names = attributes_for_create(attribute_names)
          values = attributes_with_values(attribute_names)

          im = Arel::InsertManager.new(self.class.arel_table)
          im.insert(values.transform_keys { |name| self.class.arel_table[name] })

          lock_and_insert = values["strand"] && instance_of?(Job)
          # can't use prepared statements if we're combining multiple statemenets
          sql, binds = if lock_and_insert
                         connection.unprepared_statement do
                           connection.send(:to_sql_and_binds, im)
                         end
                       else
                         connection.send(:to_sql_and_binds, im)
                       end
          sql = +sql

          if singleton && instance_of?(Job)
            sql << " ON CONFLICT (singleton) WHERE singleton IS NOT NULL AND locked_by IS NULL DO "
            sql << case on_conflict
                   when :patient, :loose
                     "NOTHING"
                   when :overwrite
                     "UPDATE SET run_at=EXCLUDED.run_at, handler=EXCLUDED.handler"
                   else # :use_earliest
                     "UPDATE SET run_at=EXCLUDED.run_at WHERE EXCLUDED.run_at<delayed_jobs.run_at"
                   end
          end

          # https://www.postgresql.org/docs/9.5/libpq-exec.html
          # https://stackoverflow.com/questions/39058213/differentiate-inserted-and-updated-rows-in-upsert-using-system-columns
          sql << " RETURNING id, (xmax = 0) AS inserted"

          if lock_and_insert
            # > Multiple queries sent in a single PQexec call are processed in a single transaction,
            # unless there are explicit BEGIN/COMMIT commands included in the query string to divide
            # it into multiple transactions.
            # but we don't need to lock when inserting into Delayed::Failed
            if values["strand"] && instance_of?(Job)
              fn_name = connection.quote_table_name("half_md5_as_bigint")
              quoted_strand = connection.quote(values["strand"].value)
              sql = "SELECT pg_advisory_xact_lock(#{fn_name}(#{quoted_strand})); #{sql}"
            end
            result = connection.execute(sql, "#{self.class} Create")
            self.id = result.values.first&.first
            inserted = result.values.first&.second
            result.clear
          else
            result = connection.exec_query(sql, "#{self.class} Create", binds)
            self.id = connection.send(:last_inserted_id, result)
            inserted = result.rows.first&.second
          end

          self.enqueue_result = if id.present? && inserted
                                  :inserted
                                elsif id.present? && !inserted
                                  :updated
                                else
                                  :dropped
                                end

          # it might not get set if there was an existing record, and we didn't update it
          if id
            @new_record = false
            changes_applied
          end

          self
        end

        def destroy
          # skip transaction and callbacks
          destroy_row
        end

        # be aware that some strand functionality is controlled by triggers on
        # the database. see
        # db/migrate/20110831210257_add_delayed_jobs_next_in_strand.rb
        #
        # next_in_strand defaults to true. if we insert a new job, and it has a
        # strand, and it's not the next in the strand, we set it to false.
        #
        # if we delete a job, and it has a strand, mark the next job in that
        # strand to be next_in_strand
        # (this is safe even if we're not deleting the job that was currently
        # next_in_strand)

        # postgresql needs this lock to be taken before the before_insert
        # trigger starts, or we risk deadlock inside of the trigger when trying
        # to raise the lock level
        before_create :lock_strand_on_create
        def lock_strand_on_create
          return unless strand.present? && instance_of?(Job)

          fn_name = self.class.connection.quote_table_name("half_md5_as_bigint")
          quoted_strand_name = self.class.connection.quote(strand)
          self.class.connection.execute("SELECT pg_advisory_xact_lock(#{fn_name}(#{quoted_strand_name}))")
        end

        # This overwrites the previous behavior
        # so rather than changing the strand and balancing at queue time,
        # this keeps the strand intact and uses triggers to limit the number running
        def self.n_strand_options(strand_name, num_strands)
          { strand: strand_name, max_concurrent: num_strands }
        end

        def self.current
          where("run_at<=?", db_time_now)
        end

        def self.future
          where("run_at>?", db_time_now)
        end

        def self.failed
          where.not(failed_at: nil)
        end

        def self.running
          where("locked_at IS NOT NULL AND locked_by<>'on hold'")
        end

        # a nice stress test:
        # 10_000.times do |i|
        #   Kernel.delay(strand: 's1', run_at: (24.hours.ago + (rand(24.hours.to_i))).system("echo #{i} >> test1.txt")
        # end
        # 500.times { |i| "ohai".delay(run_at: (12.hours.ago + (rand(24.hours.to_i))).reverse }
        # then fire up your workers
        # you can check out strand correctness: diff test1.txt <(sort -n test1.txt)
        def self.ready_to_run(forced_latency: nil)
          now = db_time_now
          now -= forced_latency if forced_latency
          where("run_at<=? AND locked_at IS NULL AND next_in_strand=?", now, true)
        end

        def self.by_priority
          order(:priority, :run_at, :id)
        end

        # When a worker is exiting, make sure we don't have any locked jobs.
        def self.clear_locks!(worker_name)
          where(locked_by: worker_name).update_all(locked_by: nil, locked_at: nil)
        end

        def self.strand_size(strand)
          where(strand: strand).count
        end

        def self.running_jobs
          running.order(:locked_at)
        end

        def self.scope_for_flavor(flavor, query)
          scope = case flavor.to_s
                  when "current"
                    current
                  when "future"
                    future
                  when "failed"
                    Delayed::Job::Failed
                  when "strand"
                    where(strand: query)
                  when "tag"
                    where(tag: query)
                  else
                    raise ArgumentError, "invalid flavor: #{flavor.inspect}"
                  end

          if %w[current future].include?(flavor.to_s)
            queue = query.presence || Delayed::Settings.queue
            scope = scope.where(queue: queue)
          end

          scope
        end

        # get a list of jobs of the given flavor in the given queue
        # flavor is :current, :future, :failed, :strand or :tag
        # depending on the flavor, query has a different meaning:
        # for :current and :future, it's the queue name (defaults to Delayed::Settings.queue)
        # for :strand it's the strand name
        # for :tag it's the tag name
        # for :failed it's ignored
        def self.list_jobs(flavor,
                           limit,
                           offset = 0,
                           query = nil)
          scope = scope_for_flavor(flavor, query)
          order = (flavor.to_s == "future") ? "run_at" : "id desc"
          scope.order(order).limit(limit).offset(offset).to_a
        end

        # get the total job count for the given flavor
        # see list_jobs for documentation on arguments
        def self.jobs_count(flavor,
                            query = nil)
          scope = scope_for_flavor(flavor, query)
          scope.count
        end

        # perform a bulk update of a set of jobs
        # action is :hold, :unhold, or :destroy
        # to specify the jobs to act on, either pass opts[:ids] = [list of job ids]
        # or opts[:flavor] = <some flavor> to perform on all jobs of that flavor
        def self.bulk_update(action, opts)
          raise("Can't #{action} failed jobs") if opts[:flavor].to_s == "failed" && action.to_s != "destroy"

          scope = if opts[:ids]
                    if opts[:flavor] == "failed"
                      Delayed::Job::Failed.where(id: opts[:ids])
                    else
                      where(id: opts[:ids])
                    end
                  elsif opts[:flavor]

                    scope_for_flavor(opts[:flavor], opts[:query])
                  end

          return 0 unless scope

          case action.to_s
          when "hold"
            scope = scope.where(locked_by: nil)
            scope.update_all(locked_by: ON_HOLD_LOCKED_BY, locked_at: db_time_now, attempts: ON_HOLD_COUNT)
          when "unhold"
            now = db_time_now
            scope = scope.where(locked_by: ON_HOLD_LOCKED_BY)
            scope.update_all([<<~SQL.squish, now, now])
              locked_by=NULL, locked_at=NULL, attempts=0, run_at=(CASE WHEN run_at > ? THEN run_at ELSE ? END), failed_at=NULL
            SQL
          when "destroy"
            scope = scope.where("locked_by IS NULL OR locked_by=?", ON_HOLD_LOCKED_BY) unless opts[:flavor] == "failed"
            scope.delete_all
          end
        end

        # returns a list of hashes { :tag => tag_name, :count => current_count }
        # in descending count order
        # flavor is :current or :all
        def self.tag_counts(flavor,
                            limit,
                            offset = 0)
          raise(ArgumentError, "invalid flavor: #{flavor}") unless %w[current all].include?(flavor.to_s)

          scope = case flavor.to_s
                  when "current"
                    current
                  when "all"
                    self
                  end

          scope = scope.group(:tag).offset(offset).limit(limit)
          scope.order(Arel.sql("COUNT(tag) DESC")).count.map { |t, c| { tag: t, count: c } }
        end

        # given a scope of non-stranded queued jobs, apply a temporary strand to throttle their execution
        # returns [job_count, new_strand]
        # (this is designed for use in a Rails console or the Canvas Jobs interface)
        def self.apply_temp_strand!(job_scope, max_concurrent: 1)
          if job_scope.where("strand IS NOT NULL OR singleton IS NOT NULL").exists?
            raise ArgumentError, "can't apply strand to already stranded jobs"
          end

          job_count = 0
          new_strand = "tmp_strand_#{SecureRandom.alphanumeric(16)}"
          ::Delayed::Job.transaction do
            job_count = job_scope.update_all(strand: new_strand, max_concurrent: max_concurrent, next_in_strand: false)
            ::Delayed::Job.where(strand: new_strand).order(:id).limit(max_concurrent).update_all(next_in_strand: true)
          end

          [job_count, new_strand]
        end

        def self.maybe_silence_periodic_log(&block)
          if Settings.silence_periodic_log
            ::ActiveRecord::Base.logger.silence(&block)
          else
            yield
          end
        end

        def self.get_and_lock_next_available(worker_names,
                                             queue = Delayed::Settings.queue,
                                             min_priority = nil,
                                             max_priority = nil,
                                             prefetch: 0,
                                             prefetch_owner: nil,
                                             forced_latency: nil)

          check_queue(queue)
          check_priorities(min_priority, max_priority)

          loop do
            jobs = maybe_silence_periodic_log do
              if connection.adapter_name == "PostgreSQL" && !Settings.select_random_from_batch
                # In Postgres, we can lock a job and return which row was locked in a single
                # query by using RETURNING. Combine that with the ROW_NUMBER() window function
                # to assign a distinct locked_at value to each job locked, when doing multiple
                # jobs in a single query.
                effective_worker_names = Array(worker_names)

                lock = nil
                lock = "FOR UPDATE SKIP LOCKED" if connection.postgresql_version >= 90_500
                target_jobs = all_available(queue,
                                            min_priority,
                                            max_priority,
                                            forced_latency: forced_latency)
                              .limit(effective_worker_names.length + prefetch)
                              .lock(lock)
                jobs_with_row_number = all.from(target_jobs)
                                          .select("id, ROW_NUMBER() OVER () AS row_number")
                updates = +"locked_by = CASE row_number "
                effective_worker_names.each_with_index do |worker, i|
                  updates << "WHEN #{i + 1} THEN #{connection.quote(worker)} "
                end
                updates << "ELSE #{connection.quote(prefetch_owner)} " if prefetch_owner
                updates << "END, locked_at = #{connection.quote(db_time_now)}"

                # Originally this was done with a subquery, but this allows the query planner to
                # side-step the LIMIT. We use a CTE here to force the subquery to be materialized
                # before running the UPDATE.
                #
                # For more details, see:
                #  * https://dba.stackexchange.com/a/69497/55285
                #  * https://github.com/feikesteenbergen/demos/blob/b7ecee8b2a79bf04cbcd74972e6bfb81903aee5d/bugs/update_limit_bug.txt
                query = <<~SQL.squish
                  WITH limited_jobs AS (#{jobs_with_row_number.to_sql})
                  UPDATE #{quoted_table_name} SET #{updates} FROM limited_jobs WHERE limited_jobs.id=#{quoted_table_name}.id
                  RETURNING #{quoted_table_name}.*
                SQL

                begin
                  jobs = find_by_sql(query)
                rescue ::ActiveRecord::RecordNotUnique => e
                  # if we got a unique violation on a singleton, it's because next_in_strand
                  # somehow got set to true on the non-running job when there is a running
                  # job. AFAICT this is not possible from inst-jobs itself, but has happened
                  # in production - either due to manual manipulation of jobs, or possibly
                  # a process in something like switchman-inst-jobs
                  raise unless e.message.include?('"index_delayed_jobs_on_singleton_running"')

                  # just repair the "strand"
                  singleton = e.message.match(/Key \(singleton\)=\((.+)\) already exists.$/)[1]
                  raise unless singleton

                  transaction do
                    # very conservatively lock the locked job, so that it won't unlock from underneath us and
                    # leave an orphaned strand
                    advisory_lock("singleton:#{singleton}")
                    locked_jobs = where(singleton: singleton).where.not(locked_by: nil).lock.pluck(:id)
                    # if it's already gone, then we're good and should be able to just retry
                    if locked_jobs.length == 1
                      updated = where(singleton: singleton, next_in_strand: true)
                                .where(locked_by: nil)
                                .update_all(next_in_strand: false)
                      raise if updated.zero?
                    end
                  end

                  retry
                end
                # because this is an atomic query, we don't have to return more jobs than we needed
                # to try and lock them, nor is there a possibility we need to try again because
                # all of the jobs we tried to lock had already been locked by someone else
                return jobs.first unless worker_names.is_a?(Array)

                result = jobs.index_by(&:locked_by)
                # all of the prefetched jobs can come back as an array
                result[prefetch_owner] = jobs.select { |j| j.locked_by == prefetch_owner } if prefetch_owner
                return result
              else
                batch_size = Settings.fetch_batch_size
                batch_size *= worker_names.length if worker_names.is_a?(Array)
                find_available(batch_size, queue, min_priority, max_priority)
              end
            end
            if jobs.empty?
              return worker_names.is_a?(Array) ? {} : nil
            end

            jobs = jobs.sort_by { rand } if Settings.select_random_from_batch
            if worker_names.is_a?(Array)
              result = {}
              jobs.each do |job|
                break if worker_names.empty?

                worker_name = worker_names.first
                if job.send(:lock_exclusively!, worker_name)
                  result[worker_name] = job
                  worker_names.shift
                end
              end
              return result
            else
              locked_job = jobs.detect do |job|
                job.send(:lock_exclusively!, worker_names)
              end
              return locked_job if locked_job
            end
          end
        end

        def self.find_available(limit,
                                queue = Delayed::Settings.queue,
                                min_priority = nil,
                                max_priority = nil)
          all_available(queue, min_priority, max_priority).limit(limit).to_a
        end

        def self.all_available(queue = Delayed::Settings.queue,
                               min_priority = nil,
                               max_priority = nil,
                               forced_latency: nil)
          min_priority ||= Delayed::MIN_PRIORITY
          max_priority ||= Delayed::MAX_PRIORITY

          check_queue(queue)
          check_priorities(min_priority, max_priority)

          ready_to_run(forced_latency: forced_latency)
            .where(priority: min_priority..max_priority, queue: queue)
            .by_priority
        end

        # Create the job on the specified strand, but only if there aren't any
        # other non-running jobs on that strand.
        # (in other words, the job will still be created if there's another job
        # on the strand but it's already running)
        def self.create_singleton(options)
          strand = options[:singleton]
          on_conflict = options.delete(:on_conflict) || :use_earliest

          transaction_for_singleton(singleton, on_conflict) do
            job = where(strand: strand, locked_at: nil).next_in_strand_order.first
            new_job = new(options)
            if job
              new_job.initialize_defaults
              job.run_at =
                case on_conflict
                when :use_earliest, :patient
                  [job.run_at, new_job.run_at].min
                when :overwrite
                  new_job.run_at
                when :loose
                  job.run_at
                end
              job.handler = new_job.handler if on_conflict == :overwrite
              job.save! if job.changed?
            else
              new_job.save!
            end
            job || new_job
          end
        end

        def self.processes_locked_locally(name: nil)
          name ||= Socket.gethostname rescue x
          where("locked_by LIKE ?", "#{name}:%").pluck(:locked_by).map { |locked_by| locked_by.split(":").last.to_i }
        end

        def self.prefetch_jobs_lock_name
          "Delayed::Job.unlock_orphaned_prefetched_jobs"
        end

        def self.unlock_orphaned_prefetched_jobs
          transaction do
            # for db performance reasons, we only need one process doing this at a time
            # so if we can't get an advisory lock, just abort. we'll try again soon
            next unless attempt_advisory_lock(prefetch_jobs_lock_name)

            horizon = db_time_now - (Settings.parent_process[:prefetched_jobs_timeout] * 4)
            where("locked_by LIKE 'prefetch:%' AND locked_at<?", horizon).update_all(locked_at: nil, locked_by: nil)
          end
        end

        def self.unlock(jobs)
          unlocked = where(id: jobs).update_all(locked_at: nil, locked_by: nil)
          jobs.each(&:unlock)
          unlocked
        end

        # Lock this job for this worker.
        # Returns true if we have the lock, false otherwise.
        #
        # It's important to note that for performance reasons, this method does
        # not re-check the strand constraints -- so you could manually lock a
        # job using this method that isn't the next to run on its strand.
        def lock_exclusively!(worker)
          now = self.class.db_time_now
          # We don't own this job so we will update the locked_by name and the locked_at
          affected_rows = self.class.where("id=? AND locked_at IS NULL AND run_at<=?", self, now).update_all(
            locked_at: now, locked_by: worker
          )
          if affected_rows == 1
            mark_as_locked!(now, worker)
            true
          else
            false
          end
        end

        def transfer_lock!(from:, to:)
          now = self.class.db_time_now
          # We don't own this job so we will update the locked_by name and the locked_at
          affected_rows = self.class.where(id: self, locked_by: from).update_all(locked_at: now, locked_by: to)
          if affected_rows == 1
            mark_as_locked!(now, to)
            true
          else
            false
          end
        end

        def mark_as_locked!(time, worker)
          self.locked_at    = time
          self.locked_by    = worker
          # We cheated ActiveRecord::Dirty with the update_all calls above, so
          # we'll fix things up here.
          if respond_to?(:changes_applied)
            changes_applied
          else
            changed_attributes["locked_at"] = time
            changed_attributes["locked_by"] = worker
          end
        end
        protected :lock_exclusively!, :mark_as_locked!

        def create_and_lock!(worker)
          raise "job already exists" unless new_record?

          # we don't want to process unique constraint violations of
          # running singleton jobs; always do it as two steps
          if singleton
            single_step_create
            lock_exclusively!(worker)
            return
          end

          self.locked_at = Delayed::Job.db_time_now
          self.locked_by = worker
          single_step_create
        end

        def fail!
          attrs = attributes
          attrs["original_job_id"] = attrs.delete("id") if Failed.columns_hash.key?("original_job_id")
          attrs["failed_at"] ||= self.class.db_time_now
          attrs.delete("next_in_strand")
          attrs.delete("max_concurrent")
          self.class.transaction do
            failed_job = Failed.create(attrs)
            destroy
            failed_job
          end
        rescue
          # we got an error while failing the job -- we need to at least get
          # the job out of the queue
          destroy
          # re-raise so the worker logs the error, at least
          raise
        end

        class Failed < Job
          include Delayed::Backend::Base
          self.table_name = :failed_jobs

          def self.cleanup_old_jobs(before_date, batch_size: 10_000)
            where("failed_at < ?", before_date).in_batches(of: batch_size).delete_all
          end

          def requeue!
            attrs = attributes.except("id",
                                      "last_error",
                                      "locked_at",
                                      "failed_at",
                                      "locked_by",
                                      "original_job_id",
                                      "requeued_job_id")
            self.class.transaction do
              job = nil
              Delayed::Worker.lifecycle.run_callbacks(:create, attrs.merge("payload_object" => payload_object)) do
                job = Job.create(attrs)
              end
              self.requeued_job_id = job.id
              save!
              JobTracking.job_created(job)
              job
            end
          end
        end
      end
    end
  end
end
