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
        log "##{Thread.current.object_id} before wait"
        s.wait
        log "###{Thread.current.object_id} after wait"
        begin
          job.run_with_lock Job::MAX_RUN_TIME, name
        rescue Exception => e
          log "ERROR: #{e}"
        ensure
          unregister_job job
          job.connection.release_connection rescue nil
        end
      end
      register_job job, t
      s.signal
      log "Launched job #{job.name}, there are #{jobs_in_execution} jobs in execution"
      return true
    end

    # Print information about the current state to stdout
    def report_jobs_state
      if jobs_in_execution > 0
        margin = 20
        title = "Jobs In Execution"
        log "#{'='*margin} #{title} #{'='*margin} "
        log " There are #{jobs_in_execution} jobs running."
        each_job_in_execution do |job, started_at, thread|
          duration = Duration.new(Time.now - started_at)
          log "\tJob #{job.id}: #{job.name}"
          log "\t   Running on #{thread} (#{thread.status}) for #{duration}"
        end
        log "#{'=' * (margin * 2 + title.size + 2)} "
      else
        log "\n\tThere is no jobs in execution right now!"
      end
    end

    # Sanity check of dead threads for precaution, but probably won't be
    # necessary
    def check_thread_sanity
      each_job_in_execution do |job, started_at, thread|
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
      each_job_in_execution {|job, x, xx| ret << job.id }
      ret
    end

    def kill_threads!
      each_job_in_execution do |job, started_at, thread|
        log "Killing #{job.name}"
        thread.terminate
      end
    end
    # ^ public methods -------------------------------------------------------
    private
    # v private methods ------------------------------------------------------

    # Whether we can or not execute this job
    def can_execute(job)
      if is_already_in_execution(job)
        log "#{job.name}: already in execution"
        return false
      end
      object = get_object(job)
      if ! object
        log "No object #{job.inspect}"
        return false
      elsif is_there_job_in_execution_for(object)
        log "#{job.name}: already a job in execution for #{object}"
        return false
      elsif jobs_in_execution >= @max_active_jobs
        log "#{job.name}: there are already many jobs in execution"
        return false
      end
      true
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
      log "<-- Let's unregister #{job.name} (#{job.id}) (#{get_object(job)})"
      @mutex.synchronize do
        @jobs.delete get_object(job)
        log "No jobs right now!" if @jobs.size == 0
      end
      log "<== Unregistered #{job.name} (#{job.id}) (#{get_object(job)})"
    end

    def register_job(job, thread)
      log "--> Let's register #{job.name} (#{job.id}) (#{get_object(job)})"
      @mutex.synchronize do
        @jobs[get_object(job)] = {
          :thread     => thread,
          :job        => job,
          :started_at => Time.now
        }
      end
      log "==> Registered #{job.name} (#{job.id}) (#{get_object(job)})"
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