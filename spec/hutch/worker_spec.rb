require 'spec_helper'
require 'hutch/worker'

describe Hutch::Worker do
  let(:setup_procs) { Array.new(3) { Proc.new {} } }
  let(:broker) { instance_double("Hutch::Broker", wait_on_threads: true, stop: true) }
  subject(:worker) { Hutch::Worker.new(broker, setup_procs) }

  describe "#run" do
    def start_kill_thread(signal)
      Thread.new do
        # sleep allows the worker time to set up the signal handling
        # before the kill signal is sent.
        sleep 0.001
        Process.kill signal, 0
      end
    end

    it "calls each setup proc" do
      setup_procs.each { |prc| expect(prc).to receive(:call) }
      start_kill_thread("INT")
      worker.run
    end

    %w(QUIT TERM INT).each do |signal|
      context "a #{signal} signal is received" do
        it "stops the broker" do
          expect(broker).to receive(:stop)

          start_kill_thread(signal)
          worker.run
        end

        it "logs that hutch is stopping" do
          expect(worker.logger).to receive(:info)
            .with("caught sig#{signal.downcase}, stopping hutch...")

          start_kill_thread(signal)
          worker.run
        end
      end
    end
  end

  describe Hutch::Worker::SHUTDOWN_SIGNALS do
    it "includes only things in Signal.list.keys" do
      expect(described_class).to eq(described_class & Signal.list.keys)
    end
  end
end
