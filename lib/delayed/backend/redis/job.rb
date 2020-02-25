# This can't currently be made compatible with redis cluster, because the Lua functions
# access keys that aren't in their keys argument list (since they pop jobs off
# a queue and then update the job with that id).

# still TODO:
#   * a consequence of our ignore-redis-failures code is that if redis is unavailable, creating delayed jobs silently fails, which is probably not what we want
#   * need a way to migrate between jobs backends
#   * we need some auditors:
#     * fail jobs in running_jobs if they've timed out
#     * have pools audit their workers and immediately fail jobs locked by dead workers (make sure this handles the restart case where two pools are running)
#     * have a master auditor that fails jobs if a whole pool dies
#     * audit strands ocasionally, look for any stuck strands where the strand queue isn't empty but there's no strand job running or queued
module Delayed::Backend::Redis
require 'delayed/backend/redis/functions'

class Job
  extend ActiveModel::Callbacks
  define_model_callbacks :create, :save
  include ActiveModel::Dirty
  include Delayed::Backend::Base
  # This redis instance needs to be set by the application during jobs configuration
  cattr_accessor :redis

  # An overview of where and when things are stored in redis:
  #
  # Jobs are given a UUID for an id, rather than an incrementing integer id.
  # The job attributes are then stored in a redis hash at job/<id>. Attribute
  # values are generally stored as their json representation, except for
  # timestamps, which as stored as floating point utc-time-since-unix-epoch
  # values, so that we can compare timestamps in Lua without a date parser.
  #
  # Jobs that are schedule to run immediately (in the present/past) are
  # inserted into the queue named queue/<queue_name>. The queue is a sorted
  # set, with the value being the job id and the weight being a floating point
  # value, <priority>.<run_at>. This formatting is key to efficient
  # querying of the next job to run.
  #
  # Jobs that are scheduled to run in the future are not inserted into the
  # queue, but rather a future queue named queue/<queue_name>/future. This
  # queue is also a sorted set, with the value being the job id, but the weight
  # is just the <run_at> value.
  #
  # If the job is on a strand, the flow is different. First, it's inserted into
  # a list named strand/<strand>. When strand jobs are inserted into the
  # current jobs queue, we check if they're next to run in the strand. If not,
  # we give them a special priority that is greater than MAX_PRIORITY, so that
  # they won't run.  When a strand job is finished, failed or deleted,
  # "tickle_strand" is called, which removes that job from the list and if that
  # job was at the front of the list, changes the priority on the next job so
  # that it's eligible to run.
  #
  # For singletons, the flow is the same as for other strand jobs, except that
  # the job is thrown out if there are already any non-running jobs in the
  # strand list.
  #
  # If a job fails, it's removed from the normal queues and inserted into the
  # failed_jobs sorted set, with job id as the value and failure time as the
  # key. The hash of job attributes is also renamed from job/<id> to
  # failed_job/<id> -- use Delayed::Job::Failed to query those jobs, same as
  # with AR jobs.
  #
  # We also insert into some other data structures for admin functionality.
  # tag_counts/current and tag_counts/all are sorted sets storing the count of
  # jobs for each tag. tag/<tag> is a set of existing job ids that have that tag.
  #
  # Most all of this happens in Lua functions, for atomicity. See the other
  # files in this directory -- functions.rb is a wrapper to call the lua
  # functions, and the individual functions are defined in .lua files in this
  # directory.

  # these key mappings are duplicated in the redis lua code, in include.lua
  module Keys
    RUNNING_JOBS = "running_jobs"
    FAILED_JOBS = "failed_jobs"
    JOB = proc { |id| "job/#{id}" }
    FAILED_JOB = proc { |id| "failed_job/#{id}" }
    QUEUE = proc { |name| "queue/#{name}" }
    FUTURE_QUEUE = proc { |name| "#{QUEUE[name]}/future" }
    STRAND = proc { |strand| strand ? "strand/#{strand}" : nil }
    TAG_COUNTS = proc { |flavor| "tag_counts/#{flavor}" }
    TAG = proc { |tag| "tag/#{tag}" }
  end

  WAITING_STRAND_JOB_PRIORITY = 2000000
  if WAITING_STRAND_JOB_PRIORITY <= Delayed::MAX_PRIORITY
    # if you change this, note that the value is duplicated in include.lua
    raise("Delayed::MAX_PRIORITY must be less than #{WAITING_STRAND_JOB_PRIORITY}")
  end

  COLUMNS = []

  # We store time attributes in redis as floats so we don't have to do
  # timestamp parsing in lua.
  TIMESTAMP_COLUMNS = []
  INTEGER_COLUMNS = []

  def self.column(name, type)
    COLUMNS << name

    if type == :timestamp
      TIMESTAMP_COLUMNS << name
    elsif type == :integer
      INTEGER_COLUMNS << name
    end

    attr_reader(name)
    define_attribute_methods([name])
    # Custom attr_writer that updates the dirty status.
    class_eval(<<-EOS, __FILE__, __LINE__ + 1)
      def #{name}=(new_value)
        #{name}_will_change! unless new_value == self.#{name}
        @#{name} = new_value
      end
    EOS
  end

  column(:id, :string)
  column(:priority, :integer)
  column(:attempts, :integer)
  column(:handler, :string)
  column(:last_error, :string)
  column(:queue, :string)
  column(:run_at, :timestamp)
  column(:locked_at, :timestamp)
  column(:failed_at, :timestamp)
  column(:locked_by, :string)
  column(:created_at, :timestamp)
  column(:updated_at, :timestamp)
  column(:tag, :string)
  column(:max_attempts, :integer)
  column(:strand, :string)
  column(:source, :string)
  column(:expires_at, :timestamp)

  def initialize(attrs = {})
    attrs.each { |k, v| self.send("#{k}=", v) }
    self.priority ||= 0
    self.attempts ||= 0
    @new_record = true
  end

  def self.instantiate(attrs)
    result = new(attrs)
    result.instance_variable_set(:@new_record, false)
    result
  end

  def self.create(attrs = {})
    result = new(attrs)
    result.save
    result
  end

  def self.create!(attrs = {})
    result = new(attrs)
    result.save!
    result
  end

  def [](key)
    send(key)
  end

  def []=(key, value)
    send("#{key}=", value)
  end

  def self.find(ids)
    if Array === ids
      find_some(ids, {})
    else
      find_one(ids, {})
    end
  end

  def new_record?
    !!@new_record
  end

  def destroyed?
    !!@destroyed
  end

  def ==(other)
    other.is_a?(self.class) && id == other.id
  end

  def hash
    id.hash
  end

  def self.reconnect!
    # redis cluster responds to reconnect directly,
    # but individual redis needs it to be called on client
    redis.respond_to?(:reconnect) ?
      redis.reconnect :
      redis.client.reconnect
  end

  def self.functions
    @@functions ||= Delayed::Backend::Redis::Functions.new(redis)
  end

  def self.find_one(id, options)
    job = self.get_with_ids([id]).first
    job || raise(ActiveRecord::RecordNotFound, "Couldn't find Job with ID=#{id}")
  end

  def self.find_some(ids, options)
    self.get_with_ids(ids).compact
  end

  def self.get_with_ids(ids)
    ids.map { |id| self.instantiate_from_attrs(redis.hgetall(key_for_job_id(id))) }
  end

  def self.key_for_job_id(job_id)
    Keys::JOB[job_id]
  end

  def self.get_and_lock_next_available(worker_name,
      queue = Delayed::Settings.queue,
      min_priority = Delayed::MIN_PRIORITY,
      max_priority = Delayed::MAX_PRIORITY,
      prefetch: nil,
      prefetch_owner: nil,
      forced_latency: nil)

    check_queue(queue)
    check_priorities(min_priority, max_priority)
    if worker_name.is_a?(Array)
      multiple_workers = true
      worker_name = worker_name.first
    end

    # as an optimization this lua function returns the hash of job attributes,
    # rather than just a job id, saving a round trip
    now = db_time_now
    now -= forced_latency if forced_latency
    job_attrs = functions.get_and_lock_next_available(worker_name, queue, min_priority, max_priority, now)
    job = instantiate_from_attrs(job_attrs) # will return nil if the attrs are blank
    if multiple_workers
      if job.nil?
        job = {}
      else
        job = { worker_name => job }
      end
    end
    job
  end

  def self.find_available(limit,
      queue = Delayed::Settings.queue,
      min_priority = Delayed::MIN_PRIORITY,
      max_priority = Delayed::MAX_PRIORITY)

    check_queue(queue)
    check_priorities(min_priority, max_priority)

    self.find(functions.find_available(queue, limit, 0, min_priority, max_priority, db_time_now))
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
    case flavor.to_s
      when 'current'
        query ||= Delayed::Settings.queue
        check_queue(query)
        self.find(functions.find_available(query, limit, offset, 0, "+inf", db_time_now))
      when 'future'
        query ||= Delayed::Settings.queue
        check_queue(query)
        self.find(redis.zrangebyscore(Keys::FUTURE_QUEUE[query], 0, "+inf", :limit => [offset, limit]))
      when 'failed'
        Failed.find(redis.zrevrangebyscore(Keys::FAILED_JOBS, "+inf", 0, :limit => [offset, limit]))
      when 'strand'
        self.find(redis.lrange(Keys::STRAND[query], offset, offset + limit - 1))
      when 'tag'
        # This is optimized for writing, since list_jobs(:tag) will only ever happen in the admin UI
        ids = redis.smembers(Keys::TAG[query])
        self.find(ids[offset, limit])
      else
        raise ArgumentError, "invalid flavor: #{flavor.inspect}"
    end
  end

  # get the total job count for the given flavor
  # flavor is :current, :future or :failed
  # for the :failed flavor, queue is currently ignored
  def self.jobs_count(flavor,
      queue = Delayed::Settings.queue)
    case flavor.to_s
      when 'current'
        check_queue(queue)
        redis.zcard(Keys::QUEUE[queue])
      when 'future'
        check_queue(queue)
        redis.zcard(Keys::FUTURE_QUEUE[queue])
      when 'failed'
        redis.zcard(Keys::FAILED_JOBS)
      else
        raise ArgumentError, "invalid flavor: #{flavor.inspect}"
    end
  end

  def self.strand_size(strand)
    redis.llen(Keys::STRAND[strand])
  end

  def self.running_jobs()
    self.find(redis.zrangebyscore(Keys::RUNNING_JOBS, 0, "+inf"))
  end

  def self.clear_locks!(worker_name)
    self.running_jobs.each do |job|
      # TODO: mark the job as failed one attempt
      job.unlock! if job.locked_by == worker_name
    end
    nil
  end

  # returns a list of hashes { :tag => tag_name, :count => current_count }
  # in descending count order
  # flavor is :current or :all
  def self.tag_counts(flavor,
      limit,
      offset = 0)
    raise(ArgumentError, "invalid flavor: #{flavor.inspect}") unless %w(current all).include?(flavor.to_s)
    key = Keys::TAG_COUNTS[flavor]
    redis.zrevrangebyscore(key, '+inf', 1, :limit => [offset, limit], :withscores => true).map { |tag, count| { :tag => tag, :count => count } }
  end

  # perform a bulk update of a set of jobs
  # action is :hold, :unhold, or :destroy
  # to specify the jobs to act on, either pass opts[:ids] = [list of job ids]
  # or opts[:flavor] = <some flavor> to perform on all jobs of that flavor
  #
  # see the list_jobs action for the list of available flavors and the meaning
  # of opts[:query] for each
  def self.bulk_update(action, opts)
    if %w(current future).include?(opts[:flavor].to_s)
      opts[:query] ||= Delayed::Settings.queue
    end
    functions.bulk_update(action, opts[:ids], opts[:flavor], opts[:query], db_time_now)
  end

  def self.create_singleton(options)
    self.create!(options.merge(:singleton => true))
  end

  def self.unlock(jobs)
    jobs.each(&:unlock!)
    jobs.length
  end

  # not saved, just used as a marker when creating
  attr_accessor :singleton, :on_conflict

  def transfer_lock!(from:, to:)
    lock_in_redis!(to)
  end

  def lock_in_redis!(worker_name)
    self.locked_at = self.class.db_time_now
    self.locked_by = worker_name
    save
  end

  def unlock!
    unlock
    save!
  end

  def save(*a)
    return false if destroyed?
    result = run_callbacks(:save) do
      if new_record?
        run_callbacks(:create) { create }
      else
        update
      end
    end
    changes_applied
    result
  end

  if Rails.version < "4.1"
    def changes_applied
      @previously_changed = changes
      @changed_attributes.clear
    end
  end

  def save!(*a)
    save(*a) || raise(RecordNotSaved)
  end

  def destroy
    self.class.functions.destroy_job(id, self.class.db_time_now)
    @destroyed = true
    freeze
  end

  # take this job off the strand, and queue up the next strand job if this job
  # was at the front
  def tickle_strand
    if strand.present?
      self.class.functions.tickle_strand(id, strand, self.class.db_time_now)
    end
  end

  def create_and_lock!(worker_name)
    raise "job already exists" unless new_record?
    lock_in_redis!(worker_name)
  end

  def fail!
    self.failed_at = self.class.db_time_now
    save!
    redis.rename Keys::JOB[id], Keys::FAILED_JOB[id]
    tickle_strand
    self
  end

  protected

  def update_queues
    if failed_at
      self.class.functions.fail_job(id)
    elsif locked_at
      self.class.functions.set_running(id)
    elsif singleton
      job_id = self.class.functions.create_singleton(id, queue, strand, self.class.db_time_now)
      # if create_singleton returns a different job id, that means this job got
      # deleted because there was already that other job on the strand. so
      # replace this job with the other for returning.
      if job_id != self.id
        singleton = self.class.find(job_id)

        self.on_conflict ||= :use_earliest
        singleton.run_at =
          case self.on_conflict
          when :use_earliest
            [singleton.run_at, run_at].min
          when :overwrite
            run_at
          when :loose
            singleton.run_at
          end
        singleton.handler = self.handler if self.on_conflict == :overwrite
        singleton.save! if singleton.changed?
        COLUMNS.each { |c| send("#{c}=", singleton.send(c)) }
      end
    else
      self.class.functions.enqueue(id, queue, strand, self.class.db_time_now)
    end
  end

  def create
    self.id ||= SecureRandom.hex(16)
    self.created_at = self.updated_at = Time.now.utc
    save_job_to_redis
    update_queues

    @new_record = false
    self.id
  end

  def update
    self.updated_at = Time.now.utc
    save_job_to_redis
    update_queues
    true
  end

  def queue_score
    "#{priority}.#{run_at.to_i}".to_f
  end

  def save_job_to_redis
    to_delete = []
    attrs = {}
    COLUMNS.each do |k|
      v = send(k)
      if v.nil?
        to_delete << k if !new_record? && changed.include?(k.to_s)
      elsif v.is_a?(ActiveSupport::TimeWithZone) || v.is_a?(Time)
        attrs[k] = v.utc.to_f
      else
        attrs[k] = v.as_json
      end
    end
    key = Keys::JOB[id]
    redis.mapped_hmset(key, attrs)
    redis.hdel(key, to_delete) unless to_delete.empty?
  end

  def self.instantiate_from_attrs(redis_attrs)
    if redis_attrs['id'].present?
      attrs = redis_attrs.with_indifferent_access
      TIMESTAMP_COLUMNS.each { |k| attrs[k] = Time.zone.at(attrs[k].to_f) if attrs[k] }
      INTEGER_COLUMNS.each { |k| attrs[k] = attrs[k].to_i if attrs[k] }
      instantiate(attrs)
    else
      nil
    end
  end

  def global_id
    id
  end

  class Failed < Job
    include Delayed::Backend::Base
    def self.key_for_job_id(job_id)
      Keys::FAILED_JOB[job_id]
    end

    def original_job_id
      id
    end
  end
end
end
