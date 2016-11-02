# Instructure Delayed Jobs

[![Build
Status](https://travis-ci.org/instructure/inst-jobs.svg?branch=master)](https://travis-ci.org/instructure/inst-jobs)

This gem was forked from
[delayed_job](https://github.com/collectiveidea/delayed_job) in late 2010. While
we have tried to maintain compatibility with delayed_job where possible, so much
code has been added and rewritten that you should approach this as a distinct
library.

It's still useful to highlight the primary differences with delayed_job, for
those familiar with it:

* `inst-jobs` was extracted from [Canvas
  LMS](https://github.com/instructure/canvas-lms), where it has been
  battle-hardened over the last 5+ years, scaling with Canvas from zero to tens
  of millions of jobs run per day.
  * To achieve this we are using some PostgreSQL specific features, which means
    support for MySQL and other ActiveRecord backends has been dropped.
  * Pushing and popping from the queue is very highly optimized, for a SQL-based
    queue. A typical PostgreSQL database running on a c3.4xlarge EC2 instance
    can handle queueing and running more than 11 million jobs per day while
    staying below 30% CPU.
  * The architecture is designed to support a mix of long-running (even
    multi-hour) jobs alongside large numbers of very short (less than one
    second) jobs.
* Daemon management is highly reliable.
  * Dead workers will be restarted, and any jobs they were working on will go
    through the normal failure handling code.
* Reliable and distributed "Cron" style jobs through the built-in [periodic
  jobs](#periodic-jobs) functionality.
* [Strands](#strands), allowing for ordered sequences of jobs based on ad-hoc
  name tags.
  * Building on the strand concept, a [singleton job](#singleton-jobs) concept
    has been added as well.
* A simple [jobs admin UI](#web-ui), usable by any Rails or Rack application.
* A separate `failed_jobs` table for tracking failed jobs.
* Automatic tracking of what code enqueued each job, if
  [Marginalia](https://github.com/basecamp/marginalia) is enabled.

## Installation

inst-jobs requires Rails 3.2 or above, and Ruby 2.0 or above. It is
tested through Rails 4.2 and Ruby 2.1.

Add this line to your Rails application's Gemfile:

```ruby
gem 'inst-jobs'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install inst-jobs

## Setup

### ActiveRecord Backend

If you are using the ActiveRecord backend, you'll need to install and
run the migrations:

    $ rake delayed_engine:install:migrations
    $ rake db:migrate

To use a separate database connection, specify it in an initializer:

```ruby
Delayed::Backend::ActiveRecord::Job.establish_connection(my_db_queue_config)
```

When upgrading `inst-jobs`, make sure to run `rake
delayed_engine:install:migrations` again to add any new migrations.

The ActiveRecord backend only supports PostgreSQL.

### Redis Backend

The redis backend doesn't require any migrations. To connect, you'll need to add
an application initializer such as `config/initializers/delayed_job.rb`:

```ruby
Delayed::Backend::Redis::Job.redis = Redis.new(url: 'redis://my-redis-host:6379/')
Delayed.select_backend(Delayed::Backend::Redis::Job)
```

While the redis backend is well-tested at the code level, it has yet to see real
use in production, as the PostgreSQL backend has scaled plenty well. If you are
interested in using this backend, please contact us.

### Worker Configuration

Worker and queue information defaults to read from `config/delayed_jobs.yml`,
this can be overridden using the `--config` option from the command line.

```yaml
development:
  workers:
  - workers: 2

production:
  workers:
  - workers: 10
```

## Usage

### Running Workers

    $ inst_jobs # display help
    $ inst_jobs start # start a worker in the background
    $ inst_jobs run # start a worker in the foreground

### Queueing Jobs

`inst-jobs` currently still uses the old `delayed_job` syntax for adding jobs to
the queue. In its simplest form, this means just calling `send_later` on any
object:

```ruby
@user.send_later(:activate!)
```

To pass parameters to the called method, add them to the `send_later` call:

```ruby
@user.send_later(:follow, other_user)
```

If a method should always be run in the background, you can call
`#handle_asynchronously` after the method declaration:

```ruby
class Device
  def deliver
    # long running method
  end
  handle_asynchronously :deliver
end

device = Device.new
device.deliver
```

#### Job Parameters

To pass parameters to the jobs engine, use the `send_later_enqueue_args` method.
If you also need to pass parameters to the called method, they go at the end:

```ruby
@user.send_later_enqueue_args(:activate!, { max_attempts: 1, priority: 50 }, other_user)
```

`handle_asynchronously` and `send_later_enqueue_args` take these parameters:

- `:priority` (number): lower numbers run first; default is 0 but can be
  reconfigured.
- `:run_at` (Time): run the job on or after this time; default is now.
- `:queue` (string): named queue to put this job in, if using separate queues.
- `:max_attempts` (number): the max number of attempts to make before
  permanently failing the job; default is 1.
- `:strand` (string): [strand](#strands) to assign this job to; default is not
  to assign to a strand.
- `:n_strand` (string): [n_strand](#n-strands) to assign this job to; default is
  none.
- `:singleton` (string): [singleton strand](#singleton-jobs) to assign this job
  to; default is none.

## Features

### Strands

A strand is a set of jobs that must be run in queue order. When a job is
assigned to a strand, it will not start running until all previous jobs assigned
to that strand have either completed or failed permanently. This is very useful
when you have sequences of jobs that need to run in order.

An example use case is the "ZIP file import" functionality in [Canvas
LMS](https://github.com/instructure/canvas-lms). Each job queued up processes an
uploaded ZIP file and updates the specified course's files. It's important to
make sure that only one import job is ever running for a course, but we don't
want to globally serialize the ZIP imports, we only want to serialize them
per-course.

Strands make this simple. We simply use the course's unique identifier as part
of the strand name, and we get the desired behavior. The (simplified) code is:

```ruby
zip_file_import.send_later_enqueue_args(:process, { strand: "zip_file_import:#{course.uuid}" })
```

Strand names are just freeform strings, and don't need to be created in advance.
The system is designed to perform well with any number of active strands.

#### N Strands

Strands are also useful when not required for correctness, but to avoid one
particular set of jobs monopolizing too many job workers. This can also be done
by using a different `:queue` parameter for the jobs, and setting up a separate
pool of workers for that queue. But this is often overkill, and can result in
wasted, idle workers for less-frequent jobs.

Another option is to use the `n_strand` parameter. This uses the same strand
functionality to cap the number of jobs that can run in parallel for the
specified `n_strand`. The limit can be changed at runtime, as well.

```ruby
# The given proc will be called each time inst-jobs queues an n_strand job, to
# determine how many jobs with this strand will be allowed to run in parallel.
Delayed::Settings.num_strands = proc do |strand_name|
  if strand_name == "external_api_call"
    3
  else
    1
  end
end

my_api.send_later_enqueue_args(:make_call, { n_strand: "external_api_call" })
```

### Singleton Jobs

Singleton jobs don't queue another job if a job is already queued with the
given strand name:

```ruby
# If a job is already queued on the strand with this name, this job will not be
# queued. It doesn't matter if previous jobs were queued on this strand but have
# already completed, it only matters what is currently on the queue.
grader.send_later_enqueue_args(:grade_student, { singleton: "grade_student:#{student.uuid}" })
```

If a job is currently running, it doesn't count as being in the queue for the
purposes of singleton jobs. This is usually the desired behavior to avoid race
conditions.

### Periodic Jobs

Periodic jobs are a reliable, distributed way of running recurring tasks, in
other words "distributed fault-tolerant Cron".

Periodic jobs need to be configured during application startup, so that workers
have access to the schedules. For instance, in a Rails app it's suggested to
create a `config/initializers/periodic_jobs.rb` file:

```ruby
# The first argument is a name tag for the job, and must be unique. The 2nd
# argument is the run schedule, in Cron syntax.
Delayed::Periodic.cron 'My Periodic Task', '30 11 * * *' do
  MyApp::SomeTask.run()
end
```

Periodic Jobs are queued just like normal jobs, and run by your same pool of
workers. Jobs are configured with `max_attempts` set to 1, so if the job fails,
it will not run again until the next scheduled interval.

### Lifecycle Events

There are several callbacks you can hook into from outside
the library, find them at the top of the "lifecycle.rb" class.

To hook into a callback, write something that looks like this in
an initializer:

```ruby
Delayed::Worker.lifecycle.before(:error) do |worker, exception|
  ErrorThingy.notify(exception)
end
```

### Work Queue

By default, each Worker process will independently query and lock jobs in the
queue. There is an experimental ParentProcess WorkQueue implementation that has
each Worker on a server communicate to a separate process on the server that
centrally handles querying and locking jobs. This can be enabled in the yml
config:

```yaml
production:
  work_queue: parent_process
```

This will cut down on DB lock contention drastically, at the cost of potentially
taking a bit longer to find new jobs. It also enables another lifecycle callback
that can be used by plugins for added functionality. This may become the default
or only implementation, eventually.

### Sentry Error Reporting

The [standard delayed_job
integration](https://github.com/getsentry/raven-ruby/blob/master/lib/raven/integrations/delayed_job.rb)
will work with inst-jobs as well. Just add in an initializer along with your
other raven-ruby configuration:

```ruby
require 'raven/integrations/delayed_job'
```

## Testing

To write tests that interact with inst-jobs, you'll need to configure
an actual ActiveRecord or Redis backend. In the future we may add an
in-memory testing backend.

### Locally

By default, if you have postgres and redis running on their default ports,
and if you have run:

```
$> createdb inst-jobs-test-1
```

Then you should be able to run the tests that come with the library with:

```
$> bundle exec rspec spec
```

### In Docker

Alternatively, if you have `docker-compose` set up, you can run the CI
build, which spins up the necessary services in docker:

```
$> ./build.sh
# or to run individual tests:
$> docker-compose build && docker-compose run --rm app rspec spec/delayed/cli_spec.rb
```

### Writing Tests

There are a few basic testing helpers available:

```ruby
require 'delayed/testing'

Delayed::Testing.drain # run all queued jobs
Delayed::Testing.run_job(job) # run a single job

before(:each) do
  Delayed::Testing.clear_all! # delete all queued jobs
end
```

## Web UI

### For Rails Apps
To use the web UI in your existing Rails application there are two options,
first "The Rails Way" as shown just below this text or the Rack way shown
at the very end of this section.

For "The Rails Way" to work there are two changes that need to be made to your
application. First you'll need to add Sinatra and `sinatra-contrib` to your
Gemfile (these dependencies are excluded from the default list so those who
aren't using this feature don't get the extra gems). Second, you'll need to
 add the following to your routes file:

```ruby
require 'delayed/server'

Rails.application.routes.draw do
  # The delayed jobs server can mounted at any route you desire, delayed_jobs is
  # just for this example
  mount Delayed::Server.new => '/delayed_jobs'
end
```

Additionally, if you wish to restrict who has access to this route it is
recommended that users wrap this route in a constraint.

### For Rack and Sinatra Apps

To use the web UI in your Rack app you can simply mount the app just like any
other Rack app in your config.ru file:

```ruby
require 'delayed/server'

# The delayed jobs server can mounted at any route you desire, delayed_jobs is
# just for this example
map '/delayed_jobs' do
  run Delayed::Server.new
end

run MyApp
```

## Contributing

1. Fork it ( https://github.com/instructure/inst-jobs/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
