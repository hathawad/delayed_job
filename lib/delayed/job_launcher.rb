require 'thread'

# Handle asynchronously launch of jobs, one per grouped value
#
# It needs the following method to be defined:
#    - name: return the name of the worker
#    - group_by: return the method to be called to obtain the object
#                needed to group jobs
#    - log: log method
#
# Implements the following interface:
#    - initialize_launcher(Fixnum max_allowed) (Must be called at beginning)
#    - launch(Delayed::Job job) => bool (launched or not)
#    - jobs_in_execution => Fixnum
#    - report_jobs_state => prints info to stdout
#    - check_thread_sanity => maintenance operation
module Delayed
  module JobLauncher
    MAX_ACTIVE_JOBS = 50

    # Initialize the launcher, you can specified the maximun number of
    # jobs executing in parallel, by default MAX_ACTIVE_JOBS constant
    #
    # The launcher has a hash with the following structure:
    # {}
    #  |- id
    #  |   `{}
    #  |     |-:thread     => Thread
    #  |     |-:job        => Delayed::Job
    #  |     `-:started_at => Time
    #  `-...
    # If group_by specified an ActiveRecord::Base object, id will be the
    # primary key of those objects.
    def initialize_launcher(max_active_jobs=MAX_ACTIVE_JOBS)
      @max_active_jobs = max_active_jobs
      @jobs = {}
      @mutex = Mutex.new
    end

    # Launch the job in a thread and register it. Returns whether the job
    # has been launched or not.
    def launch(job)
      return false unless can_execute job
      s = Semaphore.new
      t = Thread.new do
        s.wait
        begin
          job.run_with_lock Job::MAX_RUN_TIME, name
        ensure
          unregister_job job
          job.connection.release_connection rescue nil
        end
      end
      register_job job, t
      s.signal
      return true
    end

    # Print information about the current state to stdout
    def report_jobs_state
      if jobs_in_execution > 0
        margin = 20
        title = "Jobs In Execution"
        puts "\n #{'='*margin} #{title} #{'='*margin} "
        puts " There are #{jobs_in_execution} jobs running."
        each_job_in_execution do |job, started_at, thread|
          duration = Duration.new(Time.now - started_at)
          puts "\tJob #{job.id}: #{job.name}"
          puts "\t   Running on #{thread} (#{thread.status}) for #{duration}"
        end
        puts " #{'=' * (margin * 2 + title.size + 2)} "
      else
        puts "\n\tThere is no jobs in execution right now!"
      end
    end

    # Sanity check of dead threads for precaution, but probably won't be
    # necessary
    def check_thread_sanity
      @jobs.values.each do |v|
        thread = v[:thread]
        unless thread.alive?
          log "Dead thread? Terminate it!, This should not be happening"
          thread.terminate
        end
      end
    end

    # Number of jobs executing right now
    def jobs_in_execution
      @jobs.size
    end

    def jobs_ids_in_execution
      ret = []
      each_job_in_execution {|job| ret << job.id }
      ret
    end

    def kill_threads!
      each_job_in_execution do |job, started_at, thread|
        puts "Killing #{job.name}"
        thread.terminate
      end
    end
    # ^ public methods -------------------------------------------------------
    private
    # v private methods ------------------------------------------------------

    # Whether we can or not execute this job
    def can_execute(job)
      return false if is_already_in_execution(job)
      object = get_object(job)
      object && ! is_there_job_in_execution_for(object) &&
        jobs_in_execution < @max_active_jobs
    end

    def is_already_in_execution(job)
      !! @jobs.values.detect {|h| h[:job].id == job.id }
    end

    def each_job_in_execution
      @jobs.each_pair do |key, value|
        yield value[:job], value[:started_at], value[:thread]
      end
    end

    def is_there_job_in_execution_for(o)
      !! @jobs[o]
    end

    def unregister_job(job)
      @mutex.synchronize do
        @jobs.delete get_object(job)
        log "No jobs right now!" if @jobs.size == 0
      end
    end

    def register_job(job, thread)
      @mutex.synchronize do
        @jobs[get_object(job)] = {
          :thread     => thread,
          :job        => job,
         :started_at => Time.now
        }
      end
    end

    def get_object(job)
      object = job.send group_by
      if object.is_a? ActiveRecord::Base
        object.id
      else
        object
      end
    end
  end # module JobLauncher
end # module Delayed
