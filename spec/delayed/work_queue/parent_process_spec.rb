require 'spec_helper'
require 'fileutils'

RSpec.describe Delayed::WorkQueue::ParentProcess do
  before :all do
    FileUtils.mkdir_p(Delayed::Settings.expand_rails_path('tmp'))
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
    Delayed::Settings.parent_process = {
      'server_address' => '/tmp/inst-jobs-test.sock'
    }
  end

  after :all do
    Delayed.send(:remove_const, :Job)
    Delayed::Settings.parent_process = {}
  end

  after :each do
    File.unlink('/tmp/inst-jobs-test.sock') if File.exist?('/tmp/inst-jobs-test.sock')
    Delayed::Worker.lifecycle.reset!
  end

  let(:subject) { described_class.new }
  let(:worker_config) { { queue: "queue_name", min_priority: 1, max_priority: 2 } }
  let(:args) { ["worker_name", worker_config] }
  let(:job_args) { [["worker_name"], "queue_name", 1, 2] }

  describe '#initalize(config = Settings.parent_process)' do
    it 'must expand a relative path to be within the Rails root' do
      queue = described_class.new('server_address' => 'tmp/foo.sock')
      expect(queue.server_address).to eq Delayed::Settings.expand_rails_path('tmp/foo.sock')
    end

    it 'must add a file name when a relative path to a directory is supplied' do
      queue = described_class.new('server_address' => 'tmp')
      expect(queue.server_address).to eq Delayed::Settings.expand_rails_path('tmp/inst-jobs.sock')
    end

    it 'must capture a full absolute path' do
      queue = described_class.new('server_address' => '/tmp/foo.sock')
      expect(queue.server_address).to eq '/tmp/foo.sock'
    end

    it 'must add a file name when an absolute path to a directory is supplied' do
      queue = described_class.new('server_address' => '/tmp')
      expect(queue.server_address).to eq '/tmp/inst-jobs.sock'
    end
  end

  it 'generates a server listening on a valid unix socket' do
    server = subject.server
    expect(server).to be_a(Delayed::WorkQueue::ParentProcess::Server)
    expect(server.listen_socket.local_address.unix?).to be(true)
    expect { server.listen_socket.accept_nonblock }.to raise_error(IO::WaitReadable)
  end

  it 'generates a client connected to the server unix socket' do
    server = subject.server
    client = subject.client
    expect(client).to be_a(Delayed::WorkQueue::ParentProcess::Client)
    expect(client.addrinfo.unix?).to be(true)
    expect(client.addrinfo.unix_path).to eq(server.listen_socket.local_address.unix_path)
  end

  describe Delayed::WorkQueue::ParentProcess::Client do
    let(:subject) { described_class.new(addrinfo) }
    let(:addrinfo) { double('Addrinfo') }
    let(:connection) { double('Socket') }
    let(:job) { Delayed::Job.new(locked_by: "worker_name") }

    it 'marshals the given arguments to the server and returns the response' do
      expect(addrinfo).to receive(:connect).once.and_return(connection)
      expect(Marshal).to receive(:dump).with(args, connection).ordered
      expect(Marshal).to receive(:load).with(connection).and_return(job).ordered
      response = subject.get_and_lock_next_available(*args)
      expect(response).to eq(job)
    end

    it 'returns nil and then reconnects on socket error' do
      expect(addrinfo).to receive(:connect).once.and_return(connection)
      expect(Marshal).to receive(:dump).and_raise(SystemCallError.new("failure"))
      response = subject.get_and_lock_next_available(*args)
      expect(response).to be_nil

      expect(addrinfo).to receive(:connect).once.and_return(connection)
      expect(Marshal).to receive(:dump).with(args, connection)
      expect(Marshal).to receive(:load).with(connection).and_return(job)
      response = subject.get_and_lock_next_available(*args)
      expect(response).to eq(job)
    end

    it 'errors if the response is not a locked job' do
      expect(addrinfo).to receive(:connect).once.and_return(connection)
      expect(Marshal).to receive(:dump).with(args, connection)
      expect(Marshal).to receive(:load).with(connection).and_return(:not_a_job)
      expect { subject.get_and_lock_next_available(*args) }.to raise_error(Delayed::WorkQueue::ParentProcess::ProtocolError)
    end

    it 'errors if the response is a job not locked by this worker' do
      expect(addrinfo).to receive(:connect).once.and_return(connection)
      expect(Marshal).to receive(:dump).with(args, connection)
      job.locked_by = "somebody_else"
      expect(Marshal).to receive(:load).with(connection).and_return(job)
      expect { subject.get_and_lock_next_available(*args) }.to raise_error(Delayed::WorkQueue::ParentProcess::ProtocolError)
    end
  end

  describe Delayed::WorkQueue::ParentProcess::Server do
    let(:parent) { Delayed::WorkQueue::ParentProcess.new }
    let(:subject) { described_class.new(listen_socket) }
    let(:listen_socket) { Socket.unix_server_socket(parent.server_address) }
    let(:job) { :a_job }

    it 'accepts new clients' do
      client = Socket.unix(subject.listen_socket.local_address.unix_path)
      expect { subject.run_once }.to change(subject, :connected_clients).by(1)
    end

    it 'queries the queue on client request' do
      client = Socket.unix(subject.listen_socket.local_address.unix_path)
      subject.run_once

      expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return('worker_name' => job)
      Marshal.dump(args, client)
      subject.run_once
      expect(client).to be_ready
      expect(Marshal.load(client)).to eq(job)
    end

    it 'can pop multiple jobs at once' do
      client1 = Socket.unix(subject.listen_socket.local_address.unix_path)
      subject.run_once
      client2 = Socket.unix(subject.listen_socket.local_address.unix_path)
      subject.run_once

      job_args = [["worker_name1", "worker_name2"], "queue_name", 1, 2]
      jobs = { 'worker_name1' => :job1, 'worker_name2' => :job2 }

      expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return(jobs)
      Marshal.dump(["worker_name1", worker_config], client1)
      Marshal.dump(["worker_name2", worker_config], client2)
      subject.run_once
      expect(Marshal.load(client1)).to eq(:job1)
      expect(Marshal.load(client2)).to eq(:job2)
    end

    it "doesn't respond immediately if there are no jobs available" do
      client = Socket.unix(subject.listen_socket.local_address.unix_path)
      subject.run_once

      expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return({}).ordered
      Marshal.dump(args, client)
      subject.run_once
      expect(client).not_to be_ready

      # next time around, return the result
      expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return('worker_name' => job).ordered
      allow(Delayed::Settings).to receive(:sleep_delay).and_return(0)
      allow(Delayed::Settings).to receive(:sleep_delay_stagger).and_return(0)
      subject.run_once
      expect(client).to be_ready
      expect(Marshal.load(client)).to eq(job)
    end

    it 'drops the client on i/o error' do
      client = Socket.unix(subject.listen_socket.local_address.unix_path)
      subject.run_once

      Marshal.dump(args, client)

      expect(Marshal).to receive(:load).and_raise(IOError.new("socket went away"))
      expect { subject.run_once }.to change(subject, :connected_clients).by(-1)
    end

    it 'drops the client on timeout' do
      client = Socket.unix(subject.listen_socket.local_address.unix_path)
      subject.run_once

      Marshal.dump(args, client)

      expect(Marshal).to receive(:load).and_raise(Timeout::Error.new("socket timed out"))
      expect(Timeout).to receive(:timeout).with(Delayed::Settings.parent_process['server_socket_timeout']).and_yield
      expect { subject.run_once }.to change(subject, :connected_clients).by(-1)
    end

    it 'tracks when clients are idle' do
      expect(subject.all_workers_idle?).to be(true)

      client = Socket.unix(subject.listen_socket.local_address.unix_path)
      subject.run_once
      expect(subject.all_workers_idle?).to be(true)

      expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return('worker_name' => job)
      Marshal.dump(args, client)
      subject.run_once
      expect(subject.all_workers_idle?).to be(false)

      expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return({})
      Marshal.dump(args, client)
      subject.run_once
      expect(subject.all_workers_idle?).to be(true)
    end

    it 'triggers the lifecycle event around the pop' do
      called = false
      client = Socket.unix(subject.listen_socket.local_address.unix_path)
      subject.run_once

      Delayed::Worker.lifecycle.around(:work_queue_pop) do |queue, &cb|
        expect(subject.all_workers_idle?).to be(true)
        expect(queue).to eq(subject)
        expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return('worker_name' => job)
        called = true
        res = cb.call(queue)
        expect(subject.all_workers_idle?).to be(false)
        res
      end

      Marshal.dump(args, client)
      subject.run_once

      expect(Marshal.load(client)).to eq(job)
      expect(called).to eq(true)
    end
  end
end
