# `@lucid-softworks/queue-client`

Enqueue, schedule, inspect, deduplicate, and cancel durable jobs through any
`QueueStore`.

```ts
import { QueueClient } from "@lucid-softworks/queue-client";

const job = await client.enqueue(
  "send-email",
  { to: "me@example.com" },
  {
    maxAttempts: 5,
    deduplicationKey: "welcome:42",
  },
);
```

`delay` and `availableAt` create scheduled jobs. An active deduplication key
returns the existing job. IDs must be unique, priorities finite, and attempt
limits positive integers. Cancelling a terminal job is a no-op.
