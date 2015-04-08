module Delayed
  class Server
    module Helpers
      def h(text)
        Rack::Utils.escape_html(text)
      end

      def url_path(*path_parts)
        [path_prefix, path_parts].join('/').squeeze('/')
      end

      def path_prefix
        request.env['SCRIPT_NAME']
      end

      def render_javascript_env
        {
          Routes: {
            root: path_prefix,
            running: url_path('running'),
            tags: url_path('tags'),
            jobs: url_path('jobs'),
          }
        }.to_json
      end
    end
  end
end
