# KVMQ

[![deno doc](https://doc.deno.land/badge.svg)](https://deno.land/x/kvmq/mod.ts)

Library inspired by [BullMQ](https://bullmq.io) for Deno.

- Built for Deno.
- Uses Deno's KV storage.

## Example

```ts
import { Queue } from "https://deno.land/x/kvmq/mod.ts";

const db = await Deno.openKv();

const queue = new Queue<{ name: string }>(db, "test");

await queue.pushJob({ name: "Alice" });
await queue.pushJob({ name: "Bob" }, { priority: 1 });
await queue.pushJob({ name: "Kazik" }, { retryCount: 1 });
await queue.pushJob({ name: "Zenek" }, { repeatCount: 1 });

const worker = queue.createWorker(async (job) => {
  await new Promise((resolve) => setTimeout(resolve, 1000));
  console.log(`Hello ${job.state.name}!`);
});

worker.addEventListener("error", (ev) => {
  console.error(
    `Error ${ev.detail.error.message} while processing ${ev.detail.job.state.name}`,
  );
});

const controller = new AbortController();
await worker.processJobs({ signal: controller.signal });
```

See more examples in [tests](./mod.test.ts).

## TODO

- [ ] Use Deno's KV queue instead of polling.
- [ ] More tests
