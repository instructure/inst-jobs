require 'spec_helper'

RSpec.describe Delayed::Daemon do
  let(:pid_folder) { "/test/pid/folder" }
  let(:pid) { 9999 }
  let(:subject) { described_class.new(pid_folder) }

  before do
    allow(subject).to receive(:pid).and_return(pid)
  end

  describe '#stop' do
    it 'prints status if not running' do
      expect(subject).to receive(:status).with(print: false, pid: pid).and_return(false)
      expect(subject).to receive(:status).with(no_args)
      expect(Process).to receive(:kill).never
      subject.stop
    end

    it 'prints status if draining' do
      expect(subject).to receive(:status).with(print: false, pid: pid).and_return(:draining)
      expect(subject).to receive(:status).with(no_args)
      expect(Process).to receive(:kill).never
      subject.stop
    end

    it 'sends QUIT by default' do
      expect(subject).to receive(:status).with(print: false, pid: pid).and_return(:running)
      expect(subject).to receive(:puts).with(/Stopping pool/)
      expect(Process).to receive(:kill).with('QUIT', pid)
      expect(subject).to receive(:wait).with(false)
      subject.stop
    end
  end
end
