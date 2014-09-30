# Canvas Delayed Jobs

This gem is a very heavily modified fork of
[delayed_job](https://github.com/collectiveidea/delayed_job).
It used to live directly inside
[canvas-lms](https://github.com/instructure/canvas-lms),
but was extracted for use in other Rails applications.

## Features

TODO: explain the differences and additions

## Installation

canvas-jobs requires Rails 3.2 or above, and Ruby 1.9.3 or above. It is
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

### ActiveRecord Backend

If you are using the ActiveRecord backend, you'll need to install and
run the migrations:

    $ rake delayed_engine:install:migrations
    $ rake db:migrate

To use a separate database connection, specify it in an initializer:

```ruby
Delayed::Backend::ActiveRecord::Job.establish_connection(my_db_queue_config)
```

### Redis Backend

The redis backend doesn't require any migrations. By default it will
use localhost redis, to change this add an application initializer such
as `config/initializers/delayed_job.rb`:

```ruby
Delayed::Backend::Redis::Job.redis = Redis.new(url: 'redis://my-redis-host:6379/')
Delayed.select_backend(Delayed::Backend::Redis::Job)
```

### Worker Configuration

Worker and queue information is hard-coded to read from
`config/delayed_jobs.yml`, this will change in the future:

```yaml
development:
  workers:
  - workers: 2

production:
  workers:
  - workers: 10
```

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

    $ bin/rails runner 'Delayed::Pool.new.run()' # display help
    $ bin/rails runner 'Delayed::Pool.new.run()' start

(more details forthcoming, and handy script)

## Contributing

1. Fork it ( https://github.com/instructure/canvas-jobs/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
