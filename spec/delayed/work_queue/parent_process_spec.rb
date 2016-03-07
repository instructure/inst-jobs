require 'spec_helper'

RSpec.describe Delayed::WorkQueue::ParentProcess do
  before :all do
    Delayed.select_backend(Delayed::Backend::ActiveRecord::Job)
  end

  after :all do
    Delayed.send(:remove_const, :Job)
  end

  let(:subject) { described_class.new }

  it 'generates a server listening on a valid unix socket' do
    server = subject.server
    expect(server).to be_a(Delayed::WorkQueue::ParentProcess::Server)
    expect(server.listen_socket.local_address.unix?).to be_truthy
    expect { server.listen_socket.accept_nonblock }.to raise_error(IO::WaitReadable)
  end

  it 'generates a client connected to the server unix socket' do
    server = subject.server
    client = subject.client
    expect(client).to be_a(Delayed::WorkQueue::ParentProcess::Client)
    expect(client.addrinfo.unix?).to be_truthy
    expect(client.addrinfo.unix_path).to eq(server.listen_socket.local_address.unix_path)
  end

  describe Delayed::WorkQueue::ParentProcess::Client do
    let(:subject) { described_class.new(addrinfo) }
    let(:addrinfo) { double('Addrinfo') }
    let(:connection) { double('Socket') }
    let(:args) { ["worker_name", "queue_name", 1, 2] }
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
    let(:subject) { described_class.new(listen_socket) }
    let(:listen_socket) { Socket.unix_server_socket(Delayed::WorkQueue::ParentProcess.generate_socket_path) }
    let(:args) { [1,2,3] }
    let(:job) { :a_job }

    it 'accepts new clients' do
      client = Socket.unix(subject.listen_socket.local_address.unix_path)
      expect { subject.run_once }.to change(subject, :connected_clients).by(1)
    end

    it 'queries the queue on client request' do
      client, server = Socket.pair(:UNIX, :STREAM)
      expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*args).and_return(job)
      Marshal.dump(args, client)
      subject.handle_request(server)
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
      expect(Timeout).to receive(:timeout).with(Delayed::Settings.parent_process_client_timeout).and_yield
      expect { subject.run_once }.to change(subject, :connected_clients).by(-1)
    end
  end
end
