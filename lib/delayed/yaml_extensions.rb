# New definitions for YAML to aid in serialization and deserialization of delayed jobs.

require 'yaml'

# These two added domain types are for backwards compatibility with jobs created
# using the old syck tags, as syck didn't have built-in module/class dumping. We
# now use Psych's built-in tags, which are `!ruby/module` and `!ruby/class`. At
# some point we can remove these, once there are no more jobs in any queues with
# these tags.
Psych.add_domain_type("ruby/object", "Module") do |type, val|
  val.constantize
end
Psych.add_domain_type("ruby/object", "Class") do |type, val|
  val.constantize
end

# Tell YAML how to intelligently load ActiveRecord objects, using the
# database rather than just serializing their attributes to the YAML. This
# ensures the object is up to date when we use it in the job.
class ActiveRecord::Base
  def encode_with(coder)
    if id.nil?
      raise("Can't serialize unsaved ActiveRecord object for delayed job: #{self.inspect}")
    end
    coder.scalar("!ruby/ActiveRecord:#{self.class.name}", id.to_s)
  end
end

module Delayed
  module PsychExt
    module ToRuby
      def visit_Psych_Nodes_Scalar(object)
        case object.tag
        when %r{^!ruby/ActiveRecord:(.+)$}
          begin
            klass = resolve_class(Regexp.last_match[1])
            klass.unscoped.find(object.value)
          rescue ActiveRecord::RecordNotFound
            raise Delayed::Backend::RecordNotFound, "Couldn't find #{klass} with id #{object.value.inspect}"
          end
        when "!ruby/Delayed::Periodic", "!ruby/object:Delayed::Periodic"
          # The 2nd ruby/object tag is no longer generated, but some existing
          # jobs use that tag. We can remove it later once those are all gone.
          Delayed::Periodic.scheduled[object.value] || raise(NameError, "job #{object.value} is no longer scheduled")
        else
          super
        end
      end

      def resolve_class(klass_name)
        return nil if !klass_name || klass_name.empty?
        klass_name.constantize
      rescue
        super
      end
    end
  end
end

Psych::Visitors::ToRuby.prepend(Delayed::PsychExt::ToRuby)
