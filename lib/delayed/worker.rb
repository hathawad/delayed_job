module Delayed
  HIDE_BACKTRACE = true

  class Worker
    SLEEP = 60
    DEFAULT_WORKER_NAME = "host:#{Socket.gethostname} pid:#{Process.pid}" rescue "pid:#{Process.pid}"
    # Indicates that we have catched a signal and we have to exit asap
    cattr_accessor :exit
    self.exit = false

    cattr_accessor :logger
    self.logger = if defined?(Merb::Logger)
      Merb.logger
    elsif defined?(RAILS_DEFAULT_LOGGER)
      RAILS_DEFAULT_LOGGER
    end

    include JobLauncher

    # Every worker has a unique name which by default is the pid of the process (so you should
    # have only one unless override this in the constructor).
    #
    #     Thread.new { Delayed::Worker.new(:name => "Worker 1").start }
    #     Thread.new { Delayed::Worker.new(:name => "Worker 2").start }
    #
    # There are some advantages to overriding this with something which survives worker retarts:
    # Workers can safely resume working on tasks which are locked by themselves.
    # The worker will assume that it crashed before.
    attr_accessor :name

    # Constraints for this worker, what kind of jobs is gonna execute?
    attr_accessor :min_priority, :max_priority, :job_types, :only_for

    # The jobs will be group by this attribute. Each delayed_job is gonna be executed must
    # respond to `:group_by`. The jobs will be group by that and only one job can be in
    # execution.
    attr_accessor :group_by

    # Whether log, also, to stdout or not
    attr_accessor :quiet

    # Seconds to sleep between each loop running available jobs
    attr_accessor :sleep_time

    # A worker will be in a loop trying to execute pending jobs looking in the database for that
    def initialize(options={})
      [:quiet, :name, :min_priority, :max_priority, :job_types, :only_for, :group_by,
       :sleep_time
      ].each do |attr_name|
        send "#{attr_name}=", options.delete(attr_name)
      end
      # Default values
      self.name  = DEFAULT_WORKER_NAME if self.name.nil?
      self.quiet = true                if self.quiet.nil?
      self.sleep_time = SLEEP          if self.sleep_time.nil?

      @options = options
      initialize_launcher
    end

    def start
      say "===> Starting job worker #{name}"

      trap('TERM') { signal_interrupt }
      trap('INT')  { signal_interrupt }

      loop do
        if group_by
          group_by_loop
        else
          normal_loop
        end
        if self.exit
          log "Exit loop"
          break
        end
      end
      kill_threads!
    rescue Exception => e
      log "ERROR on worker loop: #{e}"
    ensure
      Job.clear_locks! name
      say "<=== Finishing job worker #{name}"
    end

    def jobs_to_execute
      Job.find_available constraints.merge(:unless => jobs_ids_in_execution)
    end

    def say(text)
      puts text unless self.quiet
      logger.info text if logger
    end
    alias :log :say

    protected

    def signal_interrupt
      if @signal && Time.now - @signal <= 1
        @signal = Time.now
        report_jobs_state
        return
      else
        now = Time.now
        @signal = now
        sleep 1
        return if @signal != now
      end
      say 'Exiting...'
      self.exit = true
    end

    def sleep_for_a_little_while
      log "Sleep #{sleep_time.to_i}"
      sleep(sleep_time.to_i) unless self.exit
      log "Sleep done"
    end

    def group_by_loop
      clean_pool_connections
      check_thread_sanity
      jobs = jobs_to_execute
      log "There are #{jobs.size} jobs to be executed (#{jobs_in_execution})"
      report_jobs_state
      jobs.each{|j| launch j }
      sleep_for_a_little_while
    end

    def normal_loop
      result = nil

      realtime = Benchmark.realtime do
        result = Job.work_off constraints
      end

      count = result.sum

      if count.zero?
        sleep_for_a_little_while
      else
        say "#{count} jobs processed at %.4f j/s, %d failed ..." % [count / realtime, result.last]
      end
    end

    def clean_pool_connections
      # Workaround for windows (at least xp and 2003 server)
      # With mysql we have overflow of connections
      ActiveRecord::Base.clear_active_connections!
      ActiveRecord::Base.verify_active_connections!
    end

    def constraints
      {:max_run_time => Job::MAX_RUN_TIME,
       :worker_name  => name,
       :min_priority => min_priority,
       :max_priority => max_priority,
       :only_for     => only_for,
       :job_types    => job_types }.merge @options
    end
  end
end
