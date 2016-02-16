# New definitions for YAML to aid in serialization and deserialization of delayed jobs.

require 'yaml'

# First, tell YAML how to load a Module. This depends on Rails .constantize and autoloading.
YAML.add_ruby_type("object:Module") do |type, val|
  val.constantize
end

Psych.add_domain_type("ruby/object", "Module") do |type, val|
  val.constantize
end

Psych.add_domain_type("ruby/object", "Class") do |type, val|
  val.constantize
end

class Module
  def to_yaml(opts = {})
    YAML.quick_emit(self.object_id, opts) do |out|
      out.scalar(taguri, name)
    end
  end
end

# Now we have to do the same for Class.
YAML.add_ruby_type("object:Class") do |type, val|
  val.constantize
end

class Class
  def to_yaml(opts = {})
    YAML.quick_emit(self.object_id, opts) do |out|
      out.scalar(taguri, name)
    end
  end

  def encode_with(coder)
    coder.scalar("!ruby/object:Class", name)
  end
end

# Now, tell YAML how to intelligently load ActiveRecord objects, using the
# database rather than just serializing their attributes to the YAML. This
# ensures the object is up to date when we use it in the job.
class ActiveRecord::Base
  yaml_as "tag:ruby.yaml.org,2002:ActiveRecord"

  def to_yaml(opts = {})
    if id.nil?
      raise("Can't serialize unsaved ActiveRecord object for delayed job: #{self.inspect}")
    end
    YAML.quick_emit(self.object_id, opts) do |out|
      out.scalar(taguri, id.to_s)
    end
  end

  def encode_with(coder)
    if id.nil?
      raise("Can't serialize unsaved ActiveRecord object for delayed job: #{self.inspect}")
    end
    coder.scalar("!ruby/ActiveRecord:#{self.class.name}", id.to_s)
  end

  def self.yaml_new(klass, tag, val)
    klass.find(val)
  rescue ActiveRecord::RecordNotFound
    raise Delayed::Backend::RecordNotFound, "Couldn't find #{klass} with id #{val.inspect}"
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
        when "tag:ruby.yaml.org,2002:Delayed::Periodic", "!ruby/Delayed::Periodic"
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

# Load Module/Class from yaml tag.
class Module
  def yaml_tag_read_class(name)
    name.constantize
    name
  end
end
