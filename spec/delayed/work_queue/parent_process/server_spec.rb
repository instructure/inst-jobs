require 'spec_helper'

RSpec.describe Delayed::WorkQueue::ParentProcess::Server do
  let(:parent) { Delayed::WorkQueue::ParentProcess.new }
  let(:subject) { described_class.new(listen_socket) }
  let(:listen_socket) { Socket.unix_server_socket(parent.server_address) }
  let(:job) { :a_job }
  let(:worker_config) { { queue: "queue_name", min_priority: 1, max_priority: 2 } }
  let(:args) { ["worker_name", worker_config] }
  let(:job_args) { [["worker_name"], "queue_name", 1, 2, hash_including(prefetch: 4)] }

  before :all do
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
  end

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

    job_args = [["worker_name1", "worker_name2"], "queue_name", 1, 2, hash_including(prefetch: 3)]
    jobs = { 'worker_name1' => :job1, 'worker_name2' => :job2 }

    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return(jobs)
    Marshal.dump(["worker_name1", worker_config], client1)
    Marshal.dump(["worker_name2", worker_config], client2)
    subject.run_once
    expect(Marshal.load(client1)).to eq(:job1)
    expect(Marshal.load(client2)).to eq(:job2)
  end

  it 'will prefetch and use jobs' do
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    allow(subject).to receive(:prefetch_owner).and_return('work_queue:X')
    job_args = [["worker_name1"], "queue_name", 1, 2, prefetch: 4, prefetch_owner: 'work_queue:X']
    job2 = Delayed::Job.new(:tag => 'tag')
    job2.create_and_lock!('work_queue:X')
    job3 = Delayed::Job.new(:tag => 'tag')
    job3.create_and_lock!('work_queue:X')
    jobs = { 'worker_name1' => :job1, 'work_queue:X' => [job2, job3]}

    expect(Delayed::Job).to receive(:get_and_lock_next_available).once.with(*job_args).and_return(jobs)
    Marshal.dump(["worker_name1", worker_config], client)
    subject.run_once
    expect(Marshal.load(client)).to eq(:job1)
    Marshal.dump(["worker_name1", worker_config], client)
    subject.run_once
    expect(Marshal.load(client)).to eq(job2)
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

  it 'drops the client when the client disconnects' do
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    Marshal.dump(args, client)

    server_client_socket = subject.clients.keys.first

    expect(server_client_socket).to receive(:eof?).and_return(true)
    expect { subject.run_once }.to change(subject, :connected_clients).by(-1)
  end

  it 'drops the client when a write fails' do
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    Marshal.dump(args, client)
    subject.run_once

    client.close

    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return('worker_name' => job)
    # the job gets unlocked
    expect(Delayed::Job).to receive(:unlock).with([job])
    subject.run_once

    # and the server removes the client from both of its internal state arrays
    expect(subject.connected_clients).to eq 0
    expect(subject.instance_variable_get(:@waiting_clients).first.last).to eq []
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

