import {
  isQueueJobTerminal,
  QueueJobNotFoundError,
  systemQueueClock,
  type QueueClock,
  type QueueEventSink,
  type QueueJob,
  type QueueStore,
} from "@lucid-softworks/queue-core";

export type QueueEnqueueOptions = Readonly<{
  id?: string;
  priority?: number;
  maxAttempts?: number;
  delay?: number;
  availableAt?: number;
  deduplicationKey?: string;
}>;

export type QueueClientOptions = Readonly<{
  clock?: QueueClock;
  events?: QueueEventSink;
  idFactory?: (name: string, now: number) => string;
}>;

let idSequence = 0;

function defaultIdFactory(name: string, now: number): string {
  return `${name}-${String(now)}-${String(++idSequence)}`;
}

export class QueueClient {
  readonly #clock: QueueClock;
  readonly #events: QueueEventSink | undefined;
  readonly #idFactory: (name: string, now: number) => string;

  constructor(
    readonly store: QueueStore,
    options: QueueClientOptions = {},
  ) {
    this.#clock = options.clock ?? systemQueueClock;
    this.#events = options.events;
    this.#idFactory = options.idFactory ?? defaultIdFactory;
  }

  async enqueue<TData>(
    name: string,
    data: TData,
    options: QueueEnqueueOptions = {},
  ): Promise<QueueJob<TData>> {
    if (name.length === 0) throw new TypeError("Job name cannot be empty");
    const priority = options.priority ?? 0;
    if (!Number.isFinite(priority))
      throw new RangeError("priority must be finite");
    const maxAttempts = options.maxAttempts ?? 1;
    if (!Number.isInteger(maxAttempts) || maxAttempts <= 0)
      throw new RangeError("maxAttempts must be a positive integer");
    const delay = options.delay ?? 0;
    if (!Number.isFinite(delay) || delay < 0)
      throw new RangeError("delay must be finite and non-negative");
    if (options.deduplicationKey !== undefined) {
      const existing = await this.store.findByDeduplicationKey(
        options.deduplicationKey,
      );
      if (existing !== undefined) return existing as QueueJob<TData>;
    }
    const now = this.#clock.now();
    const availableAt = options.availableAt ?? now + delay;
    if (!Number.isFinite(availableAt))
      throw new RangeError("availableAt must be finite");
    const job: QueueJob<TData> = {
      attempt: 0,
      availableAt,
      createdAt: now,
      data,
      id: options.id ?? this.#idFactory(name, now),
      maxAttempts,
      name,
      priority,
      state: availableAt > now ? "scheduled" : "waiting",
      updatedAt: now,
      ...(options.deduplicationKey === undefined
        ? {}
        : { deduplicationKey: options.deduplicationKey }),
    };
    if (!(await this.store.add(job)))
      throw new TypeError(`Queue job id "${job.id}" already exists`);
    this.#events?.emit({
      jobId: job.id,
      name,
      timestamp: now,
      type: job.state === "scheduled" ? "job-scheduled" : "job-enqueued",
    });
    return job;
  }

  async get(id: string): Promise<QueueJob | undefined> {
    return this.store.get(id);
  }

  async list(): Promise<readonly QueueJob[]> {
    return this.store.list();
  }

  async cancel(id: string): Promise<QueueJob | undefined> {
    const job = await this.store.get(id);
    if (job === undefined) return undefined;
    if (isQueueJobTerminal(job)) return job;
    const { lease: _lease, ...released } = job;
    const cancelled: QueueJob = {
      ...released,
      state: "cancelled",
      updatedAt: this.#clock.now(),
    };
    await this.store.save(cancelled);
    this.#events?.emit({
      jobId: job.id,
      name: job.name,
      timestamp: cancelled.updatedAt,
      type: "job-cancelled",
    });
    return cancelled;
  }

  async require(id: string): Promise<QueueJob> {
    const job = await this.get(id);
    if (job === undefined) throw new QueueJobNotFoundError(id);
    return job;
  }
}
