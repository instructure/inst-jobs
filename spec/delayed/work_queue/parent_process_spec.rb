# frozen_string_literal: true

require "spec_helper"
require "fileutils"

RSpec.describe Delayed::WorkQueue::ParentProcess do
  before :all do
    FileUtils.mkdir_p(Delayed::Settings.expand_rails_path("tmp"))
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
    Delayed::Settings.parent_process = {
      "server_address" => "/tmp/inst-jobs-test.sock"
    }
  end

  after :all do
    Delayed::Settings.parent_process = {}
  end

  after do
    FileUtils.rm_f("/tmp/inst-jobs-test.sock")
    Delayed::Worker.lifecycle.reset!
  end

  describe "#initalize(config = Settings.parent_process)" do
    it "must expand a relative path to be within the Rails root" do
      queue = described_class.new("server_address" => "tmp/foo.sock")
      expect(queue.server_address).to eq Delayed::Settings.expand_rails_path("tmp/foo.sock")
    end

    it "must add a file name when a relative path to a directory is supplied" do
      queue = described_class.new("server_address" => "tmp")
      expect(queue.server_address).to eq Delayed::Settings.expand_rails_path("tmp/inst-jobs.sock")
    end

    it "must capture a full absolute path" do
      queue = described_class.new("server_address" => "/tmp/foo.sock")
      expect(queue.server_address).to eq "/tmp/foo.sock"
    end

    it "must add a file name when an absolute path to a directory is supplied" do
      queue = described_class.new("server_address" => "/tmp")
      expect(queue.server_address).to eq "/tmp/inst-jobs.sock"
    end
  end

  it "generates a server listening on a valid unix socket" do
    server = subject.server
    expect(server).to be_a(Delayed::WorkQueue::ParentProcess::Server)
    expect(server.listen_socket.local_address.unix?).to be(true)
    expect { server.listen_socket.accept_nonblock }.to raise_error(IO::WaitReadable)
  end

  it "generates a client connected to the server unix socket" do
    server = subject.server
    client = subject.client
    expect(client).to be_a(Delayed::WorkQueue::ParentProcess::Client)
    expect(client.addrinfo.unix?).to be(true)
    expect(client.addrinfo.unix_path).to eq(server.listen_socket.local_address.unix_path)
  end
end
