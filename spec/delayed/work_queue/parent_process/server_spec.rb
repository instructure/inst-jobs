# frozen_string_literal: true

require "spec_helper"

class JobClass
  attr_reader :id

  def initialize
    @id = rand
  end

  def ==(other)
    id == other.id
  end
end

RSpec.describe Delayed::WorkQueue::ParentProcess::Server do
  subject { described_class.new(listen_socket) }

  let(:parent) { Delayed::WorkQueue::ParentProcess.new }
  let(:listen_socket) { Socket.unix_server_socket(parent.server_address) }
  let(:job) { JobClass.new }
  let(:worker_config) { { queue: "queue_name", min_priority: 1, max_priority: 2 } }
  let(:args) { ["worker_name", worker_config] }
  let(:job_args) { [["worker_name"], "queue_name", 1, 2, hash_including(prefetch: 4)] }

  before do
    Delayed::Worker.lifecycle.reset!
  end

  before :all do
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

  it "accepts new clients" do
    Socket.unix(subject.listen_socket.local_address.unix_path)
    expect { subject.run_once }.to change(subject, :connected_clients).by(1)
  end

  it "queries the queue on client request" do
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return("worker_name" => job)
    Marshal.dump(args, client)
    subject.run_once
    expect(client).to be_ready
    expect(Marshal.load(client)).to eq(job)
  end

  it "can pop multiple jobs at once" do
    client1 = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once
    client2 = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    job1 = JobClass.new
    job2 = JobClass.new
    job_args = [%w[worker_name1 worker_name2], "queue_name", 1, 2, hash_including(prefetch: 3)]
    jobs = { "worker_name1" => job1, "worker_name2" => job2 }

    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return(jobs)
    Marshal.dump(["worker_name1", worker_config], client1)
    Marshal.dump(["worker_name2", worker_config], client2)
    subject.run_once
    expect(Marshal.load(client1)).to eq(job1)
    expect(Marshal.load(client2)).to eq(job2)
  end

  it "will prefetch and use jobs" do
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    allow(subject).to receive(:prefetch_owner).and_return("work_queue:X")
    job_args = [["worker_name1"],
                "queue_name",
                1,
                2,
                { prefetch: 4, prefetch_owner: "work_queue:X", forced_latency: 6.0 }]
    job2 = Delayed::Job.new(tag: "tag")
    job2.create_and_lock!("work_queue:X")
    job3 = Delayed::Job.new(tag: "tag")
    job3.create_and_lock!("work_queue:X")
    jobs = { "worker_name1" => job, "work_queue:X" => [job2, job3] }

    expect(Delayed::Job).to receive(:get_and_lock_next_available).once.with(*job_args).and_return(jobs)
    Marshal.dump(["worker_name1", worker_config], client)
    subject.run_once
    expect(subject).not_to be_all_workers_idle
    expect(Marshal.load(client)).to eq(job)

    Marshal.dump(["worker_name1", worker_config], client)
    subject.run_once
    expect(subject).not_to be_all_workers_idle
    expect(Marshal.load(client)).to eq(job2)
  end

  context "prefetched job unlocking" do
    let(:job_args) do
      [["worker_name1"],
       "queue_name",
       1,
       2,
       { prefetch: 4, prefetch_owner: "prefetch:work_queue:X", forced_latency: 6.0 }]
    end
    let(:job2) { Delayed::Job.new(tag: "tag").tap { |j| j.create_and_lock!("prefetch:work_queue:X") } }
    let(:job3) { Delayed::Job.new(tag: "tag").tap { |j| j.create_and_lock!("prefetch:work_queue:X") } }

    before do
      client = Socket.unix(subject.listen_socket.local_address.unix_path)
      subject.run_once

      jobs = { "worker_name1" => job, "prefetch:work_queue:X" => [job2, job3] }
      allow(subject).to receive(:prefetch_owner).and_return("prefetch:work_queue:X")
      allow(Delayed::Job).to receive(:get_and_lock_next_available).once.with(*job_args).and_return(jobs)
      Marshal.dump(["worker_name1", worker_config], client)
      subject.run_once
    end

    it "doesn't unlock anything if nothing is timed out" do
      expect(Delayed::Job).not_to receive(:advisory_lock)
      expect(Delayed::Job).not_to receive(:unlock)
      subject.unlock_timed_out_prefetched_jobs
    end

    it "unlocks timed out prefetched jobs" do
      allow(Delayed::Settings).to receive(:parent_process).and_return(prefetched_jobs_timeout: -1)
      expect(Delayed::Job).to receive(:unlock).with([job2, job3])
      subject.unlock_timed_out_prefetched_jobs
      expect(subject.instance_variable_get(:@prefetched_jobs).values.sum(&:length)).to eq 0
    end

    it "fails gracefully if the lock times out" do
      allow(Delayed::Settings).to receive(:parent_process).and_return(prefetched_jobs_timeout: -1)
      expect(Delayed::Job).not_to receive(:unlock)
      expect(Delayed::Job).to receive(:advisory_lock).and_raise(ActiveRecord::QueryCanceled)
      subject.unlock_timed_out_prefetched_jobs
      expect(subject.instance_variable_get(:@prefetched_jobs).values.sum(&:length)).to eq 2
    end

    it "unlocks all jobs" do
      expect(Delayed::Job).to receive(:unlock).with([job2, job3])
      subject.unlock_all_prefetched_jobs
      expect(subject.instance_variable_get(:@prefetched_jobs).values.sum(&:length)).to eq 0
    end
  end

  it "doesn't respond immediately if there are no jobs available" do
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return({}).ordered
    Marshal.dump(args, client)
    subject.run_once
    expect(client).not_to be_ready

    # next time around, return the result
    expect(Delayed::Job).to receive(:get_and_lock_next_available)
      .with(*job_args)
      .and_return("worker_name" => job)
      .ordered
    allow(Delayed::Settings).to receive_messages(sleep_delay: 0, sleep_delay_stagger: 0)
    subject.run_once
    expect(client).to be_ready
    expect(Marshal.load(client)).to eq(job)
  end

  it "drops the client on i/o error" do
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    Marshal.dump(args, client)

    expect(Marshal).to receive(:load).and_raise(IOError.new("socket went away"))
    expect { subject.run_once }.to change(subject, :connected_clients).by(-1)
  end

  it "drops the client when the client disconnects" do
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    Marshal.dump(args, client)
    # make sure the server knows the client is waiting for a job
    subject.run_once

    client.close
    expect { subject.run_once }.to change(subject, :connected_clients).by(-1)
    expect(subject.instance_variable_get(:@waiting_clients).first.last).to eq []
  end

  it "drops the client when a write fails" do
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    Marshal.dump(args, client)
    subject.run_once

    client.close

    server_client_socket = subject.clients.keys.first
    # don't let the server see the close and process it there; we want to check a failure later
    expect(subject).to receive(:handle_request).with(server_client_socket)

    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return("worker_name" => job)
    # the job gets unlocked
    expect(Delayed::Job).to receive(:unlock).with([job])
    subject.run_once

    # and the server removes the client from both of its internal state arrays
    expect(subject.connected_clients).to eq 0
    expect(subject.instance_variable_get(:@waiting_clients).first.last).to eq []
  end

  it "tracks when clients are idle" do
    expect(subject.all_workers_idle?).to be(true)

    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once
    expect(subject.all_workers_idle?).to be(true)

    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return("worker_name" => job)
    Marshal.dump(args, client)
    subject.run_once
    expect(subject.all_workers_idle?).to be(false)

    expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return({})
    Marshal.dump(args, client)
    subject.run_once
    expect(subject.all_workers_idle?).to be(true)
  end

  it "triggers the lifecycle event around the pop" do
    called = false
    client = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once

    Delayed::Worker.lifecycle.around(:work_queue_pop) do |queue, &cb|
      expect(subject.all_workers_idle?).to be(true)
      expect(queue).to eq(subject)
      expect(Delayed::Job).to receive(:get_and_lock_next_available).with(*job_args).and_return("worker_name" => job)
      called = true
      res = cb.call(queue)
      expect(subject.all_workers_idle?).to be(false)
      res
    end

    Marshal.dump(args, client)
    subject.run_once

    expect(Marshal.load(client)).to eq(job)
    expect(called).to be(true)
  end

  it "deletes the correct worker when transferring jobs" do
    client1 = Socket.unix(subject.listen_socket.local_address.unix_path)
    client2 = Socket.unix(subject.listen_socket.local_address.unix_path)
    subject.run_once
    subject.run_once

    Marshal.dump(args, client1)
    Marshal.dump(["worker_name2", worker_config], client2)
    subject.run_once
    subject.run_once

    waiting_clients = subject.instance_variable_get(:@waiting_clients)
    expect(waiting_clients.first.last.length).to eq 2

    expect(Delayed::Job).to receive(:get_and_lock_next_available).and_return("worker_name" => job,
                                                                             "worker_name2" => job)
    subject.run_once
    expect(waiting_clients.first.last).to be_empty
  end
end
