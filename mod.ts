import { delay, retry } from "https://deno.land/std@0.201.0/async/mod.ts";
import { getLogger } from "https://deno.land/std@0.201.0/log/mod.ts";
import { ulid } from "https://deno.land/x/ulid@v0.3.0/mod.ts";

const logger = () => getLogger("kvmq");

const JOBS_KEY = "jobs" as const;

export type JobId = readonly [priority: number, ulid: string];

/**
 * Data about a job as stored in the database.
 */
export interface JobData<Input> {
  /**
   * The input value of this job.
   */
  input: Input;

  /**
   * The time at which this job can be processed.
   *
   * If the time is in the future, the job will not be processed until then,
   * even if it is at the front of the queue.
   *
   * This is set initially based on {@link JobOptions.delayMs}.
   * It is also reset on every repeat based on value of {@link repeatDelayMs}
   * and on every retry based on value of {@link retryDelayMs}.
   */
  delayUntil: Date;

  /**
   * Workers set this date when they start processing this job.
   *
   * As long as this date is in the future, the job is considered to be locked for processing by some worker.
   */
  lockUntil: Date;

  /**
   * The current progress of this job as a number between 0 and 1.
   *
   * Workers can set this value to indicate how far they are in processing this job.
   *
   * If the job is not being processed, this number is the progress it was at when it failed or was interrupted.
   *
   * Initially 0.
   */
  progress: number;

  /**
   * Number of times left to repeat this job.
   *
   * When a worker finishes processing this job, it checks this value.
   * If it's greater than 0, a new identical job is created with this value decremented.
   */
  repeatsLeft: number;

  /**
   * The amount of milliseconds to set the {@link delayUntil} value into the future when repeating this job.
   */
  repeatDelayMs: number;

  /**
   * Number of times left to attempt this job if it fails.
   *
   * If a worker fails to process this job, it checks this value.
   * If it's greater than 0, it's decremented and the job is returned to the queue.
   */
  retriesLeft: number;

  /**
   * The amount of milliseconds to set the {@link delayUntil} value into the future when retrying this job.
   */
  retryDelayMs: number;
}

/**
 * Data and metadata about a job.
 */
export interface Job<Input> extends JobData<Input> {
  id: JobId;
}

/**
 * Options for initializing a {@link Job}.
 */
export interface JobOptions {
  /**
   * Optional priority value.
   * Can be any number.
   * Default is 0.
   *
   * Jobs are sorted by priority and then by creation date, ascending.
   * Priority is negated and saved as part of the {@link Job.id}.
   */
  priority?: number;

  /**
   * Amount of milliseconds from now to wait until this job can be processed.
   *
   * A job with a delay will not start being processed even if it is at the front of the queue.
   * The other jobs behind it will be processed first until the delay expires.
   */
  delayMs?: number;

  /**
   * Number of times to repeat this job.
   *
   * Pass Infinity to repeat forever.
   * Remember that a forever repeating job is saved in the database,
   * so you should check if it already exists or clear the queue before adding it.
   *
   * Note: Repeating a job resets it's creation time, so it shows up again at the back of the queue.
   *
   * Default is 0 (do the job once).
   */
  repeatCount?: number;

  /**
   * A minimum amount of milliseconds to wait between each job repeat.
   *
   * Default is 0 (no delay).
   */
  repeatDelayMs?: number;

  /**
   * Number of times to retry this job if it fails.
   *
   * Pass Infinity to retry forever until it succeeds.
   *
   * Note: Retrying a job doesn't change it's creation time, so it still appears at the front of the queue.
   *
   * Default is 0 (attempt one time, don't retry).
   */
  retryCount?: number;

  /**
   * Amount of milliseconds to wait between each job retry.
   *
   * Default is 0 (no delay).
   */
  retryDelayMs?: number;
}

const defaultJobOptions: Required<JobOptions> = {
  priority: 0,
  delayMs: 0,
  repeatCount: 0,
  repeatDelayMs: 0,
  retryCount: 0,
  retryDelayMs: 0,
};

/**
 * Represents a job queue in the database.
 *
 * Allows listing jobs in the queue and pushing new jobs.
 *
 * @template Input The type of input that is passed to each job as an argument.
 */
export class Queue<Input> {
  /**
   * Kv database to use for storing the queue.
   */
  readonly db: Deno.Kv;

  /**
   * Key prefix to use for storing queue's data.
   */
  readonly key: Deno.KvKeyPart;

  /**
   * Initialize a job queue.
   */
  constructor(
    db: Deno.Kv,
    key: Deno.KvKeyPart,
  ) {
    this.db = db;
    this.key = key;
  }

  /**
   * Creates a new job and adds it to the queue.
   *
   * Returns job ID.
   */
  async push(input: Input, options?: JobOptions): Promise<Job<Input>> {
    const jobOptions = { ...defaultJobOptions, ...options };
    const id: JobId = [-jobOptions.priority, ulid()];
    const job: JobData<Input> = {
      input,
      delayUntil: new Date(Date.now() + (jobOptions.delayMs)),
      lockUntil: new Date(),
      progress: 0,
      repeatsLeft: jobOptions.repeatCount,
      repeatDelayMs: jobOptions.repeatDelayMs,
      retriesLeft: jobOptions.retryCount,
      retryDelayMs: jobOptions.retryDelayMs,
    };
    await this.db.set([this.key, JOBS_KEY, ...id], job);
    return { id, ...job };
  }

  /**
   * Pauses the processing of this queue.
   *
   * Sets a paused flag in the database that is checked by workers.
   *
   * A paused queue will not process new jobs until resumed,
   * but current jobs being processed will continue until they are finalized.
   */
  async pause(): Promise<void> {
    await this.db.set([this.key, "paused"], true);
  }

  /**
   * Resumes the processing of this queue.
   *
   * Resets a paused flag in the database that is checked by workers.
   */
  async resume(): Promise<void> {
    await this.db.delete([this.key, "paused"]);
  }

  /**
   * Returns an array of all jobs in this queue, from front to back.
   *
   * The jobs are sorted by priority first, then by creation date, ascending.
   *
   * Job objects don't include a `state` enum.
   * You can compute a state of a job using the following rules:
   *
   * - If {@link Job.lockUntil} is in the future, the job is being processed by a worker.
   * - If {@link Job.delayUntil} in in the future, the job can't be picked up for processing because it has a delay set.
   * - Otherwise the job is waiting to be processed.
   */
  async getAllJobs(): Promise<Array<Job<Input>>> {
    const results: Array<Job<Input>> = [];
    for await (
      const job of this.db.list<JobData<Input>>({
        prefix: [this.key, JOBS_KEY],
      })
    ) {
      const [_key, _fieldKey, priority, id] = job.key;
      if (typeof priority !== "number" || typeof id !== "string") continue;
      results.push({ id: [priority, id], ...job.value });
    }
    return results;
  }

  /**
   * Removes all jobs that aren't currently being processed from the queue.
   */
  async deleteWaitingJobs(): Promise<void> {
    for (const job of await this.getAllJobs()) {
      if (job.lockUntil > new Date()) {
        continue;
      }
      await this.deleteJob(job.id);
    }
  }

  /**
   * Removes all jobs from the queue.
   */
  async deleteJob(id: JobId): Promise<void> {
    await this.db.delete([this.key, JOBS_KEY, ...id]);
  }

  /**
   * Shorthand for constructing a {@link Worker} for this queue.
   */
  createWorker(
    handler: (value: Input) => Promise<void>,
    options?: WorkerOptions,
  ): Worker<Input> {
    return new Worker(this.db, this.key, handler, options);
  }
}

/**
 * Options for initializing a {@link Worker}.
 */
export interface WorkerOptions {
  /**
   * Maximum number of jobs to process at the same time.
   *
   * Set this to 0 to pause the worker.
   *
   * Default is 1.
   */
  concurrency?: number;

  /**
   * Time of no activity after which the job lock will be released.
   *
   * Default is 5 seconds.
   */
  lockDurationMs?: number;

  /**
   * Interval in milliseconds on which to acquire the processed job lock automatically.
   *
   * Pass Infinity to disable automatic lock renewals based on interval.
   * Locks are also always renewed on active job start and progress.
   *
   * Default is 2 seconds.
   */
  lockIntervalMs?: number;

  /**
   * Interval in milliseconds on which to check for jobs to process while idle.
   *
   * Default is 3 seconds.
   */
  pollIntervalMs?: number;
}

const defaultWorkerOptions: Required<WorkerOptions> = {
  concurrency: 1,
  lockDurationMs: 5_000,
  lockIntervalMs: 2_000,
  pollIntervalMs: 3_000,
};

/**
 * Map of events emitted by a {@link Worker}.
 */
export interface WorkerEventMap<Input> {
  /**
   * Emitted every time when processing a job fails.
   *
   * The job will be processed again if it has more attempts left.
   */
  error: CustomEvent<{ error: Error; job: Job<Input> }>;

  /**
   * Emitted every time when a job is completed.
   *
   * The job is already deleted from the queue when this event is emitted.
   * It will be added again if it has more repeats left.
   */
  complete: CustomEvent<{ job: Job<Input> }>;
}

/**
 * Represents a worker which pops the jobs from the queue and processes them.
 *
 * Remember to call {@link Worker.process} to start processing jobs.
 *
 * @template Input The type of input that is passed to each job as an argument.
 */
export class Worker<Input> extends EventTarget {
  /**
   * Kv database to use for accessing the queue.
   */
  readonly db: Deno.Kv;

  /**
   * Key prefix to use for accessing queue's data.
   */
  readonly key: Deno.KvKeyPart;

  /**
   * Other options.
   */
  options: Required<WorkerOptions>;

  #activeJobs = new Set<Promise<void>>();

  /**
   * Constructs a new worker for the given queue.
   *
   * All queue options must match the options used to construct the queue.
   * You can also use {@link Queue.createWorker} as a shorthand to construct a worker for a queue.
   *
   * This constructor is useful if your worker is in separate process from the queue.
   */
  constructor(
    db: Deno.Kv,
    key: Deno.KvKeyPart,
    public handler: (value: Input) => Promise<void>,
    options: WorkerOptions = {},
  ) {
    super();
    this.db = db;
    this.key = key;
    this.options = { ...defaultWorkerOptions, ...options };
  }

  /**
   * Starts processing jobs.
   *
   * Pass an abort signal to stop processing jobs at a later time.
   * Aborting will wait for the current jobs to finish processing.
   *
   * Returns a promise that resolves when the worker is aborted and all already started jobs are finalized.
   *
   * This can reject when getting or updating jobs in the database fails.
   * Whenever an error occurs in the job handler, the worker will emit an `error` event.
   */
  async process(signal?: AbortSignal): Promise<void> {
    logger().debug(`Queue ${this.key}: Processing started`);
    while (true) {
      // check if concurrency limit was reached
      if (this.#activeJobs.size >= this.options.concurrency) {
        // reached concurrency limit
        if (this.#activeJobs.size > 0) {
          // wait for a job to finish
          await Promise.race(this.#activeJobs);
        } else {
          // concurrency is 0 (worker is paused)
          await delay(this.options.pollIntervalMs);
          continue;
        }
      }

      // break the loop if aborted
      if (signal?.aborted) {
        break;
      }

      // check if the queue is paused
      const pausedEntry = await this.db.get<boolean>([this.key, "paused"]);
      if (pausedEntry.value) {
        await delay(this.options.pollIntervalMs);
        continue;
      }

      // find a job to process
      let nextJobEntry: Deno.KvEntry<JobData<Input>> | undefined = undefined;
      for await (
        const jobEntry of this.db.list<JobData<Input>>({
          prefix: [this.key, JOBS_KEY],
        })
      ) {
        if (jobEntry.value.lockUntil > new Date()) {
          // still locked for processing, skip
          continue;
        }
        if (jobEntry.value.delayUntil > new Date()) {
          // still delayed, skip
          continue;
        }
        nextJobEntry = jobEntry;
        break;
      }
      // check if a job was not found
      if (!nextJobEntry) {
        await delay(this.options.pollIntervalMs);
        continue;
      }
      const jobEntry = nextJobEntry;

      // check job ID
      const [_key, _fieldKey, priority, id] = jobEntry.key;
      if (typeof priority !== "number" || typeof id !== "string") {
        // invalid key, delete and skip
        await this.db.delete(jobEntry.key);
        continue;
      }
      const jobId: JobId = [priority, id];

      // mark this job as locked for processing
      const lockResult = await this.db.atomic().check(jobEntry).set(
        jobEntry.key,
        {
          ...jobEntry.value,
          lockUntil: new Date(Date.now() + this.options.lockDurationMs),
          progress: 0,
        },
      ).commit();
      if (!lockResult.ok) {
        // someone else locked this job before us, skip
        continue;
      }

      // start the interval that will keep the job locked
      const lockInterval = setInterval(async () => {
        try {
          await retry(async () => {
            const currentJobEntry = await this.db.get<JobData<Input>>(
              jobEntry.key,
            );
            if (currentJobEntry.versionstamp == null) return;
            await this.db.atomic().check(currentJobEntry).set(
              jobEntry.key,
              {
                ...currentJobEntry.value,
                lockUntil: new Date(Date.now() + this.options.lockDurationMs),
              } satisfies JobData<Input>,
            ).commit();
          });
        } catch {
          // ignore lock errors
          // if it fails too many times, the job will just return to the queue
        }
      }, this.options.lockIntervalMs);

      // start processing the job
      const jobPromise = (async () => {
        // process the job
        logger().info(`Queue ${this.key}: Job ${jobId.join(":")}: Started`);
        await this.handler(jobEntry.value.input);
        logger().info(`Queue ${this.key}: Job ${jobId.join(":")}: Completed`);

        // get current job data
        const currentJobEntry = await this.db.get<JobData<Input>>(
          jobEntry.key,
        );

        if (!currentJobEntry.value) {
          // job was deleted, nothing to do
          return;
        }

        // delete the job
        await this.db.delete(currentJobEntry.key);

        // dispatch complete event
        this.dispatchEvent(
          new CustomEvent("complete", {
            detail: {
              job: { id: jobId, ...currentJobEntry.value ?? jobEntry.value },
            },
          }) satisfies WorkerEventMap<Input>["complete"],
        );

        // check if the job should be repeated
        if (currentJobEntry.value.repeatsLeft > 0) {
          logger().debug(
            `Queue ${this.key}: Job ${
              jobId.join(":")
            }: Repeating ${currentJobEntry.value.repeatsLeft} more times`,
          );
          await this.db.set(
            [this.key, JOBS_KEY, priority, ulid()],
            {
              ...currentJobEntry.value,
              lockUntil: new Date(),
              delayUntil: new Date(
                Date.now() + currentJobEntry.value.repeatDelayMs,
              ),
              progress: 0,
              repeatsLeft: currentJobEntry.value.repeatsLeft - 1,
            } satisfies JobData<Input>,
          );
        }
      })()
        // handle processing error
        .catch(async (error) => {
          try {
            logger().warning(
              `Queue ${this.key}: Job ${jobId.join(":")}: Failed: ${error}`,
            );

            // get current job data
            const currentJobEntry = await this.db.get<JobData<Input>>(
              jobEntry.key,
            );

            // dispatch error event
            this.dispatchEvent(
              new CustomEvent("error", {
                detail: {
                  error,
                  job: {
                    id: jobId,
                    ...currentJobEntry.value ?? jobEntry.value,
                  },
                },
              }) satisfies WorkerEventMap<Input>["error"],
            );

            if (!currentJobEntry.value) {
              // job was deleted, nothing to do
              return;
            }

            // check if the job should be retried
            if (currentJobEntry.value.retriesLeft > 0) {
              logger().debug(
                `Queue ${this.key}: Job ${
                  jobId.join(":")
                }: Retrying ${currentJobEntry.value.retriesLeft} more times`,
              );
              await this.db.set(
                currentJobEntry.key,
                {
                  ...currentJobEntry.value,
                  delayUntil: new Date(
                    Date.now() + currentJobEntry.value.retryDelayMs,
                  ),
                  lockUntil: new Date(),
                  progress: 0,
                  retriesLeft: currentJobEntry.value.retriesLeft - 1,
                } satisfies JobData<Input>,
              );
            } else {
              // delete the job
              await this.db.delete(currentJobEntry.key);
            }
          } catch {
            // ignore errors
          }
        })
        // handle job completion
        .finally(() => {
          clearInterval(lockInterval);
          this.#activeJobs.delete(jobPromise);
        });

      this.#activeJobs.add(jobPromise);
    }
    logger().debug(`Queue ${this.key}: Processing stopped`);
  }

  /**
   * See {@link WorkerEventMap} for available events.
   */
  addEventListener<K extends keyof WorkerEventMap<Input>>(
    type: K,
    listener: (this: Worker<Input>, ev: WorkerEventMap<Input>[K]) => void,
    options?: boolean | AddEventListenerOptions,
  ): void;
  addEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject,
    options?: boolean | AddEventListenerOptions,
  ): void;
  addEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions,
  ): void {
    super.addEventListener(type, listener, options);
  }
}
