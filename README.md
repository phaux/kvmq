# KVMQ

[![deno doc](https://doc.deno.land/badge.svg)](https://deno.land/x/kvmq/mod.ts)

Library inspired by [BullMQ](https://bullmq.io) for Deno.

- Built for Deno.
- Uses Deno's KV storage.

## Example

```ts
import { Queue } from "https://deno.land/x/kvmq/mod.ts";

const queue = new Queue<{ name: string }>(db, "test");

await queue.push({ name: "Alice" });
await queue.push({ name: "Bob" }, { priority: 1 });
await queue.push({ name: "Kazik" }, { retryCount: 1 });
await queue.push({ name: "Zenek" }, { repeatCount: 1 });

const worker = queue.createWorker(async (input) => {
  console.log(`Hello ${input.name}!`);
});

worker.addEventListener("error", (ev) => {
  console.error(
    `Error ${ev.detail.error.message} while processing ${ev.detail.job.input.name}`,
  );
});

const controller = new AbortController();
await worker.process(controller.signal);
```

## TODO

- [ ] Use Deno's KV queue instead of polling.
