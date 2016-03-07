# Canvas Delayed Jobs

[![Build
Status](https://travis-ci.org/instructure/canvas-jobs.svg?branch=master)](https://travis-ci.org/instructure/canvas-jobs)

This gem is a very heavily modified fork of
[delayed_job](https://github.com/collectiveidea/delayed_job).
It used to live directly inside
[canvas-lms](https://github.com/instructure/canvas-lms),
but was extracted for use in other Rails applications.

## Features

TODO: explain the differences and additions

## Installation

canvas-jobs requires Rails 3.2 or above, and Ruby 2.0 or above. It is
tested through Rails 4.2 and Ruby 2.1.

Add this line to your Rails application's Gemfile:

```ruby
gem 'canvas-jobs'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install canvas-jobs

## Usage

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

### ActiveRecord Backend

If you are using the ActiveRecord backend, you'll need to install and
run the migrations:

    $ rake delayed_engine:install:migrations
    $ rake db:migrate

To use a separate database connection, specify it in an initializer:

```ruby
Delayed::Backend::ActiveRecord::Job.establish_connection(my_db_queue_config)
```

The ActiveRecord backend only supports PostgreSQL.

### Redis Backend

The redis backend doesn't require any migrations. To connect, you'll need to add an
application initializer such as `config/initializers/delayed_job.rb`:

```ruby
Delayed::Backend::Redis::Job.redis = Redis.new(url: 'redis://my-redis-host:6379/')
Delayed.select_backend(Delayed::Backend::Redis::Job)
```

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

### Periodic Jobs

Periodic jobs need to be configured during application startup, so that
workers have access to the schedules. For instance, create a
`config/initializers/periodic_jobs.rb`:

```ruby
Delayed::Periodic.cron 'Alerts::DelayedAlertSender.process', '30 11 * * *' do
  Alerts::DelayedAlertSender.process
end
```

### Running Workers

    $ canvas_job # display help
    $ canvas_job start # start a worker in the background
    $ canvas_job run # start a worker in the foreground


### Testing

To write tests that interact with canvas-jobs, you'll need to configure
an actual ActiveRecord or Redis backend. In the future we may add an
in-memory testing backend.

By default, if you have postgres and redis running on their default ports,
and if you have run:

```
$> createdb canvas-jobs-test-1
```

Then you should be able to run the tests that come with the library with:

```
$> bundle exec rspec spec
```

There are a few basic testing helpers available:

```ruby
require 'delayed/testing'

Delayed::Testing.drain # run all queued jobs
Delayed::Testing.run_job(job) # run a single job

before(:each) do
  Delayed::Testing.clear_all! # delete all queued jobs
end
```

### Web UI

#### For Rails Apps
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

#### For Rack and Sinatra Apps
To use the web UI in your Rack app you can simply mount the app just like any other Rack
app in your config.ru file:

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

1. Fork it ( https://github.com/instructure/canvas-jobs/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
