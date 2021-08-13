# frozen_string_literal: true

shared_examples_for "Delayed::PerformableMethod" do
  it "does not ignore ActiveRecord::RecordNotFound errors because they are not always permanent" do
    story = Story.create text: "Once upon..."
    p = Delayed::PerformableMethod.new(story, :tell)
    story.destroy
    expect { YAML.load(p.to_yaml) }.to raise_error(Delayed::Backend::RecordNotFound)
  end

  it "stores the object using native YAML even if its an active record" do
    story = Story.create text: "Once upon..."
    p = Delayed::PerformableMethod.new(story, :tell)
    expect(p.class).to   eq(Delayed::PerformableMethod)
    expect(p.object).to  eq(story)
    expect(p.method).to  eq(:tell)
    expect(p.args).to    eq([])
    expect(p.perform).to eq("Once upon...")
  end

  it "allows class methods to be called on ActiveRecord models" do
    Story.create!(text: "Once upon a...")
    p = Delayed::PerformableMethod.new(Story, :count)
    expect { expect(p.send(:perform)).to be 1 }.not_to raise_error
  end

  it "allows class methods to be called" do
    p = Delayed::PerformableMethod.new(StoryReader, :reverse, args: ["ohai"])
    expect { expect(p.send(:perform)).to eq("iaho") }.not_to raise_error
  end

  it "allows module methods to be called" do
    p = Delayed::PerformableMethod.new(MyReverser, :reverse, args: ["ohai"])
    expect { expect(p.send(:perform)).to eq("iaho") }.not_to raise_error
  end

  it "stores arguments as native YAML if they are active record objects" do
    story = Story.create text: "Once upon..."
    reader = StoryReader.new
    p = Delayed::PerformableMethod.new(reader, :read, args: [story])
    expect(p.class).to   eq(Delayed::PerformableMethod)
    expect(p.method).to  eq(:read)
    expect(p.args).to    eq([story])
    expect(p.perform).to eq("Epilog: Once upon...")
  end

  it "deeplies de-AR-ize arguments in full name" do
    story = Story.create text: "Once upon..."
    reader = StoryReader.new
    p = Delayed::PerformableMethod.new(reader, :read, args: [["arg1", story, { [:key, 1] => story }]])
    expect(p.full_name).to eq(
      "StoryReader#read([\"arg1\", Story.find(#{story.id}), {[:key, 1] => Story.find(#{story.id})}])"
    )
  end

  it "calls the on_failure callback" do
    story = Story.create text: "wat"
    p = Delayed::PerformableMethod.new(story, :tell, on_failure: :text=)
    p.send(:on_failure, "fail")
    expect(story.text).to eq("fail")
  end

  it "calls the on_permanent_failure callback" do
    story = Story.create text: "wat"
    p = Delayed::PerformableMethod.new(story, :tell, on_permanent_failure: :text=)
    p.send(:on_permanent_failure, "fail_frd")
    expect(story.text).to eq("fail_frd")
  end

  it "can still generate a name with no kwargs" do
    story = Story.create text: "wat"
    p = Delayed::PerformableMethod.new(story, :tell, kwargs: nil)
    expect(p.full_name).to eq("Story.find(#{story.id}).tell()")
  end
end
