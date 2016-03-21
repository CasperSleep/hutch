require 'hutch/logging'

module Hutch
  class Worker
    include Logging

    SHUTDOWN_SIGNALS = %w(QUIT TERM INT)

    def initialize(broker, setup_procs)
      self.broker        = broker
      self.setup_procs = setup_procs
      self.sig_read, self.sig_write = IO.pipe
    end

    # Run the main event loop. The consumers will be set up with queues, and
    # process the messages in their respective queues indefinitely. This method
    # never returns.
    def run
      setup_procs.each(&:call)
      register_signal_handlers
      wait_for_signal
      stop
    end

    private

    attr_accessor :broker, :setup_procs, :sig_read, :sig_write

    def wait_for_signal
      IO.select([sig_read])
    end

    # Stop a running worker by killing all subscriber threads.
    def stop
      sig = sig_read.gets.strip.downcase
      logger.info "caught sig#{sig}, stopping hutch..."

      broker.stop
    end

    def register_signal_handlers
      SHUTDOWN_SIGNALS.each do |sig|
        # This needs to be reentrant, so we queue up signals to be handled
        # in the run loop, rather than acting on signals here
        trap(sig) do
          sig_write.puts(sig)
        end
      end
    end
  end
end
