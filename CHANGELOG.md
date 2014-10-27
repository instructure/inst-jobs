# Change Log
All notable changes to this project will be documented in this file.

## 0.9.6 - 2014-10-27
### Added
- A convenient bin/canvas_job script to install with binstubs.

### Changed
- Fix logging to stdout in foreground mode in Rails 4.
- Fix redis backend bugs introduced by the gem extraction.
- Allow private methods to be the target of send_later in newer versions
  of Ruby.
- Fix unlocking of failed jobs.

## 0.9.5 - 2014-10-22
### Added
- Exceptional_exit callback for when a worker fully dies.
- Plug into rails class reloading in development mode, to reload
  application code between jobs.

## 0.9.4 - 2014-10-21
### Added
- ERB parsing of the delayed_jobs.yml worker config file.
- Basic Delayed::Testing test helpers.

### Changed
- Remove the (not completely working) default redis connection.

## 0.9.3 - 2014-09-30
### Changed
- Fix installing on ruby 1.9 by removing syck dependency in gemspec.

## 0.9.2 - 2014-09-30
### Changed
- Redis compatibility and reconnecting fixes.

## 0.9.0 - 2014-09-30
### Added
- Initial release of the standalone gem.

