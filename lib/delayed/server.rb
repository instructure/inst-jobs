require 'sinatra/base'
require 'sinatra/json'
require 'json'
require 'delayed_job'

module Delayed
  class Server < Sinatra::Base
    APP_DIR = File.dirname(File.expand_path(__FILE__))
    set :views,         File.join(APP_DIR, 'server', 'views')
    set :public_folder, File.join(APP_DIR, 'server', 'public')

    def initialize(*args, &block)
      super()
      # Rails will take care of establishing the DB connection for us if there is
      # an application present
      if using_active_record? && !ActiveRecord::Base.connected?
        ActiveRecord::Base.establish_connection(ENV['DATABASE_URL'])
      end

      @allow_update = args.length > 0 && args[0][:update]
    end

    def using_active_record?
      Delayed::Job == Delayed::Backend::ActiveRecord::Job
    end

    def allow_update
      @allow_update
    end

    # Ensure we're connected to the DB before processing the request
    before do
      if ActiveRecord::Base.respond_to?(:verify_active_connections!) && using_active_record?
        ActiveRecord::Base.verify_active_connections!
      end
    end

    # Release any used connections back to the pool
    after do
      ActiveRecord::Base.clear_active_connections! if using_active_record?
    end

    configure :development do
      require 'sinatra/reloader'
      register Sinatra::Reloader
    end

    helpers do
      # this can't get required until the class has been opened for the first time
      require 'delayed/server/helpers'
      include Delayed::Server::Helpers
    end

    get '/' do
      erb :index
    end

    get '/running' do
      content_type :json
      json({
        draw: params['draw'].to_i,
        recordsTotal: Delayed::Job.running.count,
        recordsFiltered: Delayed::Job.running.count,
        data: Delayed::Job.running_jobs.map{ |j|
          j.as_json(include_root: false, except: [:handler, :last_error])
        },
      })
    end

    get '/tags' do
      content_type :json
      json({
        draw: params['draw'].to_i,
        data: Delayed::Job.tag_counts('current', 10)
      })
    end

    DEFAULT_PAGE_SIZE = 10
    MAX_PAGE_SIZE = 100
    get '/jobs' do
      content_type :json
      flavor = params['flavor'] || 'current'
      page_size = extract_page_size
      offset = Integer(params['start'] || 0)
      case flavor
      when 'id'
        jobs = Delayed::Job.where(id: params['search_term'])
        total_records = 1
      when 'future', 'current', 'failed'
        jobs = Delayed::Job.list_jobs(flavor, page_size, offset)
        total_records =  Delayed::Job.jobs_count(flavor)
      else
        query = params['search_term']
        if query.present?
          jobs = Delayed::Job.list_jobs(flavor, page_size, offset, query)
        else
          jobs = []
        end
        total_records =  Delayed::Job.jobs_count(flavor, query)
      end
      json({
        draw: params['draw'].to_i,
        recordsTotal: total_records,
        recordsFiltered: jobs.size,
        data: build_jobs_json(jobs),
      })
    end

    post '/bulk_update' do
      content_type :json

      halt 403 unless @allow_update

      payload = JSON.parse(request.body.read).symbolize_keys
      Delayed::Job.bulk_update(payload[:action], { ids: payload[:ids] })

      json({
        success: true
      })
    end

    private

    def extract_page_size
      page_size = Integer(params['length'] || DEFAULT_PAGE_SIZE)
      # if dataTables wants all of the records it will send us -1 but we don't
      # want the potential to kill our servers with this request so we'll limit it
      page_size = DEFAULT_PAGE_SIZE if page_size == -1
      [page_size, MAX_PAGE_SIZE].min
    end


    def build_jobs_json(jobs)
      json = jobs.map{ |j|
        j.as_json(root: false, except: [:handler, :last_error])
      }
    end
  end
end
