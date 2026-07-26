import { QueueEventBus } from "@lucid-softworks/queue-events";
import { MemoryQueueStore } from "@lucid-softworks/queue-store-memory";
import { describe, expect, it } from "vitest";

import { QueueClient } from "../src/index.js";

describe("QueueClient", () => {
  it("enqueues immediate and scheduled jobs with events and generated ids", async () => {
    let now = 10;
    const events = new QueueEventBus();
    const store = new MemoryQueueStore();
    const client = new QueueClient(store, {
      clock: { now: () => now, sleep: async () => undefined },
      events,
    });
    const immediate = await client.enqueue("work", { value: 1 });
    expect(immediate.id).toMatch(/^work-10-/);
    expect(immediate.state).toBe("waiting");
    now = 20;
    const scheduled = await client.enqueue("later", "data", {
      delay: 5,
      id: "scheduled",
      maxAttempts: 3,
      priority: 2,
    });
    expect(scheduled).toMatchObject({
      availableAt: 25,
      maxAttempts: 3,
      priority: 2,
      state: "scheduled",
    });
    expect(events.history.map(({ type }) => type)).toEqual([
      "job-enqueued",
      "job-scheduled",
    ]);
    expect(await client.get("scheduled")).toEqual(scheduled);
    expect(await client.list()).toHaveLength(2);
    expect(await client.require("scheduled")).toEqual(scheduled);
    await expect(client.require("missing")).rejects.toThrow("was not found");
  });

  it("deduplicates, rejects duplicate ids, and validates enqueue options", async () => {
    const defaults = new QueueClient(new MemoryQueueStore());
    expect(
      (await defaults.enqueue("default", null, { id: "default" })).state,
    ).toBe("waiting");
    const store = new MemoryQueueStore();
    const client = new QueueClient(store, {
      clock: { now: () => 0, sleep: async () => undefined },
      idFactory: () => "generated",
    });
    const first = await client.enqueue("work", 1, {
      deduplicationKey: "same",
    });
    expect(await client.enqueue("work", 2, { deduplicationKey: "same" })).toBe(
      first,
    );
    await expect(
      client.enqueue("other", null, { id: "generated" }),
    ).rejects.toThrow("already exists");
    await expect(client.enqueue("", null)).rejects.toThrow(TypeError);
    await expect(
      client.enqueue("x", null, { priority: Number.NaN }),
    ).rejects.toThrow(RangeError);
    await expect(client.enqueue("x", null, { maxAttempts: 0 })).rejects.toThrow(
      RangeError,
    );
    await expect(client.enqueue("x", null, { delay: -1 })).rejects.toThrow(
      RangeError,
    );
    await expect(
      client.enqueue("x", null, { availableAt: Number.POSITIVE_INFINITY }),
    ).rejects.toThrow(RangeError);
  });

  it("cancels live jobs while preserving missing and terminal jobs", async () => {
    let now = 0;
    const events = new QueueEventBus();
    const store = new MemoryQueueStore();
    const client = new QueueClient(store, {
      clock: { now: () => now, sleep: async () => undefined },
      events,
      idFactory: () => "job",
    });
    await client.enqueue("work", null);
    const active = store.claim({
      leaseDuration: 10,
      now,
      workerId: "worker",
    });
    now = 2;
    expect((await client.cancel(active?.id as string))?.state).toBe(
      "cancelled",
    );
    expect((await client.cancel("job"))?.state).toBe("cancelled");
    expect(await client.cancel("missing")).toBeUndefined();
    expect(events.history.at(-1)?.type).toBe("job-cancelled");
  });
});
