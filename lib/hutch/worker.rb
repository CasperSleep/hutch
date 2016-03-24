module Hutch
  class Worker

    def run
      start
      MainLoop.loop_until_signalled
      stop
    end

    private

    def start
      Hutch.connect
      Config[:setup_procs].each(&:call)
    end

    def stop
      Hutch.broker.stop
    end
  end
end
