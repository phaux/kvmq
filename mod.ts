import { delay, retry } from "https://deno.land/std@0.201.0/async/mod.ts";
import { getLogger } from "https://deno.land/std@0.201.0/log/mod.ts";
import { ulid } from "https://deno.land/x/ulid@v0.3.0/mod.ts";

const logger = () => getLogger("kvmq");

const JOBS_KEY = "jobs" as const;

export type JobId = readonly [priority: number, ulid: string];

/**
 * Data about a job as stored in the database.
 *
 * @template State Type of custom state data that is passed to the worker when processing this job.
 */
export interface JobData<State> {
  /**
   * Any data that is passed to the worker when processing this job.
   */
  state: State;

  /**
   * The time at which this job can be processed.
   *
   * A job with a delay will not start being processed even if it is at the front of the queue.
   * The other jobs behind it will be processed first until the delay expires.
   *
   * This value is reset on every repeat based on value of {@link repeatDelayMs}
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
   * Number of times to repeat this job.
   *
   * When a worker finishes processing this job, it checks this value.
   * If it's greater than 0, a new identical job is created with this value decremented.
   * Repeating a job resets it's creation time, so it shows up again at the back of the queue.
   *
   * Pass Infinity to repeat forever.
   * Remember that a forever repeating job is saved in the database,
   * so you should check if it already exists or clear the queue before adding it.
   */
  repeatCount: number;

  /**
   * Minimum amount of milliseconds to wait between each job repeat.
   *
   * Sets the {@link delayUntil} value this amount into the future when repeating this job.
   */
  repeatDelayMs: number;

  /**
   * Number of times to attempt this job if it fails.
   *
   * If a worker fails to process this job, it checks this value.
   * If it's greater than 0, it's decremented and the job is returned to the queue.
   * Retrying a job doesn't change it's creation time, so it still appears at the front of the queue.
   */
  retryCount: number;

  /**
   * Amount of milliseconds to wait between each job retry.
   *
   * Sets the {@link delayUntil} value this amount into the future when retrying this job.
   */
  retryDelayMs: number;
}

/**
 * Data and metadata about a job.
 *
 * @template Input Type of custom state data that is passed to the worker when processing this job.
 */
export interface Job<Input> extends JobData<Input> {
  id: JobId;
}

/**
 * Options for initializing a {@link Job}.
 */
export interface JobOptions
  extends Partial<Omit<JobData<unknown>, "state" | "lockUntil">> {
  /**
   * Optional priority value.
   * Can be any number.
   * Default is 0.
   *
   * Jobs are sorted by priority and then by creation date, ascending.
   * Priority is negated and saved as part of the {@link Job.id}.
   */
  priority?: number;
}

/**
 * Represents a job queue in the database.
 *
 * Allows listing jobs in the queue and pushing new jobs.
 *
 * @template State Type of custom state data that is passed to the worker when processing a job.
 */
export class Queue<State> {
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
  async pushJob(state: State, options: JobOptions = {}): Promise<Job<State>> {
    const {
      priority = 0,
      delayUntil = new Date(),
      repeatCount = 0,
      repeatDelayMs = 0,
      retryCount = 0,
      retryDelayMs = 0,
    } = options;
    const id: JobId = [-priority, ulid()];
    const job: JobData<State> = {
      state,
      delayUntil,
      lockUntil: new Date(),
      repeatCount,
      repeatDelayMs,
      retryCount,
      retryDelayMs,
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
  async getAllJobs(): Promise<Array<Job<State>>> {
    const results: Array<Job<State>> = [];
    for await (
      const job of this.db.list<JobData<State>>({
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
   *
   * TODO: make this atomic.
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
   * Removes a job from the queue.
   *
   * TODO: throw if locked.
   */
  async deleteJob(id: JobId): Promise<void> {
    await this.db.delete([this.key, JOBS_KEY, ...id]);
  }

  /**
   * Listens for queue updates.
   *
   * Note: currently it just polls the queue every few seconds.
   */
  async listenUpdates(
    onUpdate: (jobs: Array<Job<State>>) => void,
    options: { signal?: AbortSignal; pollIntervalMs?: number },
  ): Promise<void> {
    const { signal, pollIntervalMs = 3000 } = options;
    let lastJobsIds = "";
    while (true) {
      if (signal?.aborted) break;

      const jobs = await this.getAllJobs();
      const jobsIds = jobs.map((job) => job.id.join(":")).join();
      if (jobsIds !== lastJobsIds) {
        onUpdate(jobs);
        lastJobsIds = jobsIds;
      }

      await delay(pollIntervalMs);
    }
  }

  /**
   * Shorthand for constructing a {@link Worker} for this queue.
   */
  createWorker(
    handler: (value: HandlerParams<State>) => Promise<void>,
    options?: WorkerOptions,
  ): Worker<State> {
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
   * Time of no activity in milliseconds after which the job lock will be released.
   *
   * Default is 5 seconds.
   */
  lockDurationMs?: number;

  /**
   * Interval in milliseconds on which to acquire the processed job lock automatically.
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

/**
 * Map of events emitted by a {@link Worker}.
 *
 * @template Input Type of custom state data that is passed to the worker when processing a job.
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
 * Parameters received by worker's handler function.
 *
 * @template State Type of custom state data that is passed to the handler.
 */
export interface HandlerParams<State> {
  /**
   * The current state of the job.
   */
  state: State;

  /**
   * Sets the state of the current job.
   *
   * This can reject if updating value in the database fails.
   */
  setState: (state: State) => Promise<void>;

  /**
   * Stops processing any more jobs.
   *
   * This is the same as calling {@link Worker.stopProcessing}.
   */
  stopProcessing: () => void;
}

/**
 * Represents a worker that processes jobs from a queue.
 *
 * Remember to call {@link processJobs} to start processing jobs.
 *
 * @template State Type of custom state data that is passed to the worker when processing a job.
 */
export class Worker<State> extends EventTarget {
  /**
   * Kv database to use for accessing the queue.
   */
  readonly db: Deno.Kv;

  /**
   * Key prefix to use for accessing queue's data.
   */
  readonly key: Deno.KvKeyPart;

  /**
   * The function that processes the jobs.
   */
  handler: (params: HandlerParams<State>) => Promise<void>;

  /**
   * Worker options.
   */
  options: Required<WorkerOptions>;

  /**
   * Promise for finishing currently running processors.
   */
  #processingFinished = Promise.resolve();

  /**
   * Whether the worker is currently processing jobs.
   */
  #isProcessing = false;

  /**
   * Abort controller for stopping currently running processors.
   */
  #processingController = new AbortController();

  /**
   * Set of currently running jobs as promises.
   */
  readonly #activeJobs = new Set<Promise<void>>();

  /**
   * Constructs a new worker for the given queue.
   *
   * DB and key must match the ones used to construct the queue.
   * You can also use {@link Queue.createWorker} as a shorthand to construct a worker for a queue.
   *
   * This constructor is useful if your worker is in separate process from the queue.
   */
  constructor(
    db: Deno.Kv,
    key: Deno.KvKeyPart,
    handler: (params: HandlerParams<State>) => Promise<void>,
    options: WorkerOptions = {},
  ) {
    super();
    this.db = db;
    this.key = key;
    this.handler = handler;
    this.options = {
      concurrency: options.concurrency ?? 1,
      lockDurationMs: options.lockDurationMs ?? 5_000,
      lockIntervalMs: options.lockIntervalMs ?? 2_000,
      pollIntervalMs: options.pollIntervalMs ?? 3_000,
    };
  }

  /**
   * Starts processing jobs.
   *
   * If you already called this method and it's still running,
   * the current call will first wait for previous one to finish.
   *
   * Pass an abort signal to stop processing jobs at a later time.
   * Aborting won't wait for the already started jobs to finish processing.
   * To also wait for all currently running jobs, use `await Promise.all(worker.activeJobs)`.
   *
   * Returns a promise that resolves when the processing finishes.
   * It can reject when getting or updating jobs in the database fails.
   * Whenever an error occurs in the processing handler, the worker will emit an `error` event.
   */
  processJobs(options: { signal?: AbortSignal } = {}): Promise<void> {
    const { signal } = options;
    const controller = this.#processingController;
    this.#processingFinished = this.#processingFinished.then(() =>
      this.#processJobsLoop({ signal, controller })
    );
    return this.#processingFinished;
  }

  async #processJobsLoop(
    options: { signal?: AbortSignal; controller: AbortController },
  ) {
    const { signal, controller } = options;

    logger().debug(`Queue ${this.key}: Processing started`);
    this.#isProcessing = true;
    try {
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
        if (signal?.aborted || controller.signal.aborted) {
          break;
        }

        // check if the queue is paused
        const pausedEntry = await this.db.get<boolean>([this.key, "paused"]);
        if (pausedEntry.value) {
          await delay(this.options.pollIntervalMs);
          continue;
        }

        // find a job to process
        let nextJobEntry: Deno.KvEntry<JobData<State>> | undefined = undefined;
        for await (
          const jobEntry of this.db.list<JobData<State>>({
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
        // check if no job was found
        if (!nextJobEntry) {
          await delay(this.options.pollIntervalMs);
          continue;
        }
        const jobEntry = nextJobEntry;

        // mark this job as locked for processing
        const lockResult = await this.db.atomic().check(jobEntry).set(
          jobEntry.key,
          {
            ...jobEntry.value,
            lockUntil: new Date(Date.now() + this.options.lockDurationMs),
          } satisfies JobData<State>,
        ).commit();
        if (!lockResult.ok) {
          // someone else locked this job before us, skip
          continue;
        }

        // start the interval that will keep the job locked
        const lockInterval = setInterval(async () => {
          try {
            const currentJobEntry = await this.db.get<JobData<State>>(
              jobEntry.key,
            );
            if (currentJobEntry.versionstamp == null) return;
            const lockResult = await this.db.atomic().check(currentJobEntry)
              .set(
                jobEntry.key,
                {
                  ...currentJobEntry.value,
                  lockUntil: new Date(Date.now() + this.options.lockDurationMs),
                } satisfies JobData<State>,
              ).commit();
            if (!lockResult.ok) {
              throw new Error(`Atomic update failed`);
            }
          } catch (error) {
            // ignore lock errors
            // if it fails too many times, the job will just return to the queue
            logger().warning(
              `Job ${jobEntry.key.join("/")}: Failed to update lock: ${error}`,
            );
          }
        }, this.options.lockIntervalMs);

        // start processing the job
        const jobPromise = this.#processJob(jobEntry).finally(() => {
          clearInterval(lockInterval);
          this.#activeJobs.delete(jobPromise);
        });

        this.#activeJobs.add(jobPromise);
      }
    } finally {
      this.#isProcessing = false;
    }
    logger().info(`Queue ${this.key}: Processing stopped`);
  }

  async #processJob(jobEntry: Deno.KvEntry<JobData<State>>): Promise<void> {
    try {
      logger().info(`Job ${jobEntry.key.join("/")}: Started`);

      // process the job
      await this.handler({
        state: jobEntry.value.state,
        setState: async (state) => {
          // save new state to database and update lock
          await retry(async () => {
            const currentJobEntry = await this.db.get<JobData<State>>(
              jobEntry.key,
            );
            if (currentJobEntry.versionstamp == null) {
              // TODO: don't retry on not found
              throw new Error(`Entry not found`);
            }
            const setStateResult = await this.db.atomic().check(currentJobEntry)
              .set(
                jobEntry.key,
                {
                  ...currentJobEntry.value,
                  state,
                  lockUntil: new Date(Date.now() + this.options.lockDurationMs),
                } satisfies JobData<State>,
              ).commit();
            if (!setStateResult.ok) {
              throw new Error(`Atomic update failed`);
            }
          });
        },
        stopProcessing: () => {
          this.stopProcessing();
        },
      });

      // processing job finished
      logger().info(`Job ${jobEntry.key.join("/")}: Completed`);

      // get current job data
      const finishedJobEntry = await this.db.get<JobData<State>>(jobEntry.key);

      if (!finishedJobEntry?.value) {
        // job was deleted somehow, nothing to do
        return;
      }

      // delete the job
      await this.db.delete(finishedJobEntry.key);

      // dispatch complete event
      // TODO: allow changing repeat count in the event handler?
      this.dispatchEvent(
        new CustomEvent("complete", {
          detail: {
            job: {
              id: [Number(jobEntry.key[2]), String(jobEntry.key[3])],
              ...finishedJobEntry.value,
            },
          },
        }) satisfies WorkerEventMap<State>["complete"],
      );

      // check if the job should be repeated
      if (finishedJobEntry.value.repeatCount > 0) {
        logger().info(
          `Job ${jobEntry.key.join("/")}: ` +
            `Repeating ${finishedJobEntry.value.repeatCount} more times`,
        );
        await this.db.set(
          [...finishedJobEntry.key.slice(0, 3), ulid()],
          {
            ...finishedJobEntry.value,
            lockUntil: new Date(),
            delayUntil: new Date(
              Date.now() + finishedJobEntry.value.repeatDelayMs,
            ),
            repeatCount: finishedJobEntry.value.repeatCount - 1,
          } satisfies JobData<State>,
        );
      }
    } catch (error) {
      logger().error(`Job ${jobEntry.key.join("/")}: Failed: ${error}`);

      // get current job data
      const failedJobEntry = await this.db.get<JobData<State>>(jobEntry.key)
        .catch(() => null);

      if (!failedJobEntry?.value) {
        // something went wrong or job was deleted somehow, nothing to do
        return;
      }

      // dispatch error event
      // TODO: allow changing retry count in the event handler?
      this.dispatchEvent(
        new CustomEvent("error", {
          detail: {
            error,
            job: {
              id: [Number(jobEntry.key[2]), String(jobEntry.key[3])],
              ...failedJobEntry.value,
            },
          },
        }) satisfies WorkerEventMap<State>["error"],
      );

      try {
        // check if the job should be retried
        if (failedJobEntry.value.retryCount > 0) {
          logger().info(
            `Job ${jobEntry.key.join("/")}: ` +
              `Retrying ${failedJobEntry.value.retryCount} more times`,
          );
          await retry(async () => {
            const currentJobEntry = await this.db.get<JobData<State>>(
              failedJobEntry.key,
            );
            if (currentJobEntry.versionstamp == null) {
              // TODO: don't retry on not found
              throw new Error(`Entry not found`);
            }
            const retryResult = await this.db.atomic().check(currentJobEntry)
              .set(
                currentJobEntry.key,
                {
                  ...currentJobEntry.value,
                  delayUntil: new Date(
                    Date.now() + currentJobEntry.value.retryDelayMs,
                  ),
                  lockUntil: new Date(),
                  retryCount: currentJobEntry.value.retryCount - 1,
                } satisfies JobData<State>,
              ).commit();
            if (!retryResult.ok) {
              throw new Error(`Atomic update failed`);
            }
          });
        } else {
          // delete the job
          await this.db.delete(jobEntry.key);
        }
      } catch (error) {
        // ignore errors that might happen when updating a failed job
        // if it happens the job will just return to the queue as stalled
        logger().error(
          `Job ${jobEntry.key.join("/")}: Failed to retry: ${error}`,
        );
      }
    }
  }

  /**
   * Promise for finishing currently running processors.
   *
   * This promise gets replaced with a new one every time {@link processJobs} is called.
   * If you call and forget {@link processJobs}, you can use this to get the promise again and await it.
   *
   * This doesn't include the jobs that already started processing.
   * To wait for them too use {@link activeJobs}.
   */
  get processingFinished(): Promise<void> {
    return this.#processingFinished;
  }

  /**
   * Whether the worker is currently processing jobs.
   */
  get isProcessing(): boolean {
    return this.#isProcessing;
  }

  /**
   * Set of promises for finishing jobs that are currently being processed.
   *
   * When jobs are finished, they remove themselves from this set.
   *
   * To check the number of currently running jobs, use `worker.activeJobs.size()`.
   * To wait for all currently running jobs to finish, use `await Promise.all(worker.activeJobs)`.
   */
  get activeJobs(): ReadonlySet<Promise<void>> {
    return this.#activeJobs;
  }

  /**
   * Aborts all currently running processors.
   *
   * This is an alternative to passing an abort signal to {@link processJobs}.
   */
  stopProcessing(): void {
    this.#processingController.abort();
    this.#processingController = new AbortController();
  }

  /**
   * See {@link WorkerEventMap} for available events.
   */
  addEventListener<K extends keyof WorkerEventMap<State>>(
    type: K,
    listener: (this: Worker<State>, ev: WorkerEventMap<State>[K]) => void,
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
