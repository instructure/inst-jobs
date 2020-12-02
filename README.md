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

* `inst-jobs` was adapted for [Canvas
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

inst-jobs requires Rails 3.2 or above, and Ruby 2.1 or above. It is
tested through Rails 5.0 and Ruby 2.3.

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

An initializer can also be used to set preferred values for any
settings that control specific interal delayed job behavior:

```ruby
Delayed::Settings.max_attempts              = 1
Delayed::Settings.queue                     = "canvas_queue"
Delayed::Settings.sleep_delay               = ->{ 2.0 }
```

You can find a list of available settings in `lib/delayed_job/settings.rb`.

## Usage

### Signal Handling

Inst-jobs makes an attempt at being well behaved with respect to how child
processes are handled. When the pool receives SIGQUIT it will pass that on
and wait `Settings.slow_exit_timeout` seconds (default 20) for all children
to finish their currently active task and exit. If they take longer than
this a SIGTERM will be sent telling them to clean up and bail quickly, if
that doesn't happen within 2 seconds SIGKILL is then sent. This graceful exit
can be expedited by sending SIGTERM/SIGINT to the pool, this will still allow
the `slow_exit_timeout` period for the workers to exit but they should exit
almost immediately.

The old behavior of the pool exiting and leaving the child processes orphaned
can be preserved by setting `kill_workers_on_exit` to false. This will cause
the first signal sent to the pool to be propagated to all of the child
processes after which the pool will exit.

### Running Workers

    $ inst_jobs # display help
    $ inst_jobs start # start a worker in the background
    $ inst_jobs run # start a worker in the foreground

### Queueing Jobs

In the simplest form, this means just calling `delay` on any
object before calling the actual method:

```ruby
@user.delay.activate!
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

To pass parameters to the jobs engine, send them to the  `delay` method:

```ruby
@user.delay(max_attempts: 1, priority: 50).activate!(other_user)
```

`handle_asynchronously` and `delay` take these parameters:

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
- `:on_conflict` (:use_earliest|:overwrite|:loose): option for how to handle the
  new job if a singleton[#singleton-jobs] job of the same type already exists.

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
zip_file_import.delay(strand: "zip_file_import:#{course.uuid}").process
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

my_api.delay(n_strand: "external_api_call").make_call
```

### Singleton Jobs

Singleton jobs don't queue another job if a job is already queued with the
given strand name:

```ruby
# If a job is already queued on the strand with this name, this job will not be
# queued. It doesn't matter if previous jobs were queued on this strand but have
# already completed, it only matters what is currently on the queue.
grader.delay(singleton: "grade_student:#{student.uuid}").grade_student
```

If a job is currently running, it doesn't count as being in the queue for the
purposes of singleton jobs. This is usually the desired behavior to avoid race
conditions.

You can also pass an `on_conflict` option. The default of `:use_earliest` means
that the queued job will be updated to the earliest `run_at` of the existing and
the new job. Assuming you're using the default `run_at` of now, that means the
new job will simply be dropped. It can also be used if you run the singleton on
a schedule (like a periodic job), but occasionally want it to run now.

The second option is `:overwrite` and will always update the pending job to
use the `run_at` of the new job. This is useful for "debouncing" - you have some
cleanup that needs to run after a trigger action, but there are many of that
trigger action and it's not useful to run the single cleanup job until the
trigger action calms down. This is also useful if the arguments to the job
might change, and you want it to run with the latest version of those
arguments.

The third option is `:loose`. This is similar to the default use with a
`run_at` of now, but does _not_ lock the strand in order to guarantee exactly
one of the singleton is in queue. It does the query to see if a job is already
in queue, and if it is, does nothing. This means there is a race condition
that multiple processes might see no queued job, and each enqueue one,
meaning it's not a true singleton. But it also reduces locking on the queue
itself, and is useful for a singleton that is triggered with high frequency,
and low impact if it happens to run a couple extra times. Because it is
less sure about the state of the queue, it cannot implement the
`:use_earliest` logic and update the already queued job. Therefore it is not
viable if you want to mix on-demand and periodic singletons.

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

This works by storing a registry of periodic jobs with their intervals,
enqueueing each job immediately for the next time it should be run
at, and then having an extra step after the job is performed
that re-enqueues it for the NEXT time it will run.  It's expected
that every periodic job will be in the queue all the time (either executing
or queued for the next time it will execute).

By default, Periodic jobs are singletons (see docs above on singleton jobs).
If you really don't want a periodic job to be a singleton, you can pass
{ singleton: false } as a job arg.  This makes it _possible_ for multiple
versions of this job to run at the same time in the rare cases where that's
appropriate.

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

### Worker Health Checking and Unlocking Orphaned Jobs

Occasionally a worker will unexpectedly terminate without being allowed to run
any cleanup code, when this happens it causes any jobs that process had locked
to remain locked indefinitely. To alleviate this each worker can register itself
with a locally running consul agent which will watch that each process is still
alive, when a process is found to be dead it will automatically be deregistered
from the agent causing another process to come along and reschedule the locked job.


#### Configuring the Consul health check

In order to use the Consul health check you must include the `imperium` gem,
version 0.2.3 or newer, in your application's Gemfile. It is not included in the
default dependencies because it is an optional feature.

```ruby
# Enable the consul health check
Setting.worker_health_check_type = :consul

# Configure the health check
Setting.worker_health_check = {
  service_name: 'canvas-worker', # Optional, defaults to 'inst-jobs_worker'
  check_interval: '7m', # Optional, defaults to 5m
}

# Schedule a periodic job to clean up abandoned jobs
Delayed::Periodic.cron 'abandoned job cleanup', '*/10 * * * *', {singleton: false} do
  Delayed::Worker::HealthCheck.reschedule_abandoned_jobs
end
```

Notice that the abandoned job cleanup should be scheduled with "singleton: false".
Remember back in the Periodic Jobs docs where we talked about this being possible
in the rare cases you didn't want a periodic job to be a singleton?  This is one
of those times.  If the rescheduling job dies while running, there is no other
cleanup job to cleanup itself.  Therefore the "reschedule_abandoned_jobs"
method takes care of it's own concurrency control with postgres advisory locks.

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
```

#### Running individual tests in Docker

This repo uses `rvm` to run specs under a variety of ruby versions (specifically by following the matrix
defined in .travis.yml using the gem "wwtd").
For local testing, you probably just want to get things tested under _some_ ruby version.
Here's how.

First, you'll want a persistent gems volume, which you can get by:

```
$> cp docker-compose.override.yml.example docker-compose.override.yml
```

Then you can install bundler and gems, which you'll want to do in your ruby version of choice:

```
$> docker-compose run --rm app bash -lc "rvm-exec 2.7 gem install bundler -v 1.17.3"
$> docker-compose run --rm app bash -lc "rvm-exec 2.7 bundle"
```

Now, to run an individual spec:

```
$> docker-compose run --rm app bash -lc "rvm-exec 2.7 bundle exec rspec spec/delayed/worker_spec.rb"
```

You can also run the whole suite, but under just one rvm context, with:

```
$> docker-compose run --rm app bash -lc "rvm-exec 2.7 bundle exec rake spec"
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

inst-jobs has a built-in web ui that allows users to view running jobs.
Optionally, this web ui can support basic job management as well (hold, unhold,
and delete operations are supported).  To enable this feature, pass a hash
containing `update: true` into the `Delayed::Server` constructor.  You probably
want to ensure that the jobs endpoint requires authentication before enabling
this feature.

### For Rails Apps
To use the web UI in your existing Rails application there are two options,
first "The Rails Way" as shown just below this text or the Rack way shown
at the very end of this section.

For "The Rails Way" to work there are two changes that need to be made to your
application.

First you'll need to add Sinatra and `sinatra-contrib` to your
Gemfile (these dependencies are excluded from the default list so those who
aren't using this feature don't get the extra gems). For Rails 5.x applications,
you'll need to use Sinatra 2.x, which is in beta at the time of this writing.

Second, you'll need to add the following to your routes file:

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


## Publishing

Ready to release a new version of inst-jobs?
Make sure you're an owner (https://rubygems.org/gems/inst-jobs)

If your rubygems credentials are already set in `~/.gem/credentials`,
you can just run the release task:
`bundle exec rake release`

If they are not, you can do this manually for now, and it will
cache your credentials as part of the process:

```bash
bundle exec rake build
# -> inst-jobs VERSION built to pkg/inst-jobs-VERSION.gem
gem push pkg/inst-jobs-VERSION.gem
# -> follow prompts to enter your login information
```

Future releases you can now just use the release rake task,
although if you have MFA enabled (and you should!) and your
MFA valid period expires, you'll have to do the gem push
manually to enter a new MFA code.
