# frozen_string_literal: true

module Kernel
  def sender(idx = 0)
    frame_self = nil
    # 3. one for the block, one for this method, one for the method calling this
    # method, and _then_ we get to the self for who sent the message we want
    RubyVM::DebugInspector.open { |dc| frame_self = dc.frame_self(3 + idx) }
    frame_self
  end
end
