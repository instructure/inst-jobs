$:.unshift('lib')

require 'delayed/server'

map '/delayed_jobs' do
  run Delayed::Server.new
end
