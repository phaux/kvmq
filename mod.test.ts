import { handlers, setup } from "https://deno.land/std@0.201.0/log/mod.ts";
import { Queue } from "./mod.ts";
import { assertEquals } from "https://deno.land/std@0.135.0/testing/asserts.ts";
import { delay } from "https://deno.land/std@0.201.0/async/delay.ts";

setup({
  handlers: {
    console: new handlers.ConsoleHandler("DEBUG"),
  },
  loggers: {
    kvmq: { level: "DEBUG", handlers: ["console"] },
  },
});

const db = await Deno.openKv();

Deno.test("queue", async () => {
  const queue = new Queue<string>(db, "test");
  await queue.deleteWaitingJobs();

  await queue.push("a");
  await queue.push("b", { priority: 1 });
  await queue.push("error", { retryCount: 1 });
  await queue.push("c", { repeatCount: 1 });
  await queue.push("error");
  await queue.push("d");

  const jobs = await queue.getAllJobs();

  assertEquals(jobs.length, 6);

  const results: string[] = [];
  const errors: string[] = [];

  const worker = queue.createWorker(async (input) => {
    console.log("processing", input);
    await delay(1000);
    if (input === "error") {
      throw new Error("error");
    }
    results.push(input);
  });

  worker.addEventListener("error", (e) => {
    errors.push(e.detail.error.message);
  });

  const controller = new AbortController();
  worker.process(controller.signal);

  await delay(500);
  // initial state
  assertEquals(results, []);
  assertEquals(errors, []);
  await delay(1000);
  // b is processed first because it has the highest priority
  assertEquals(results, ["b"]);
  assertEquals(errors, []);
  await delay(1000);
  // a is processed next
  assertEquals(results, ["b", "a"]);
  assertEquals(errors, []);
  await delay(1000);
  // error is next, should be attempted and fail 2 times
  assertEquals(results, ["b", "a"]);
  assertEquals(errors, ["error"]);
  await delay(1000);
  assertEquals(results, ["b", "a"]);
  assertEquals(errors, ["error", "error"]);
  await delay(1000);
  // c is next, first repeat
  assertEquals(results, ["b", "a", "c"]);
  assertEquals(errors, ["error", "error"]);
  await delay(1000);
  // second error
  assertEquals(results, ["b", "a", "c"]);
  assertEquals(errors, ["error", "error", "error"]);
  await delay(1000);
  // d
  assertEquals(results, ["b", "a", "c", "d"]);
  assertEquals(errors, ["error", "error", "error"]);
  await delay(1000);
  // second repeat of c (repeats re-add at the end of the queue)
  assertEquals(results, ["b", "a", "c", "d", "c"]);
  assertEquals(errors, ["error", "error", "error"]);
  await delay(1000);
  // end, nothing changed
  assertEquals(results, ["b", "a", "c", "d", "c"]);
  assertEquals(errors, ["error", "error", "error"]);
  // now waiting 3 seconds before polling again
  await queue.push("error", { retryCount: 1, retryDelayMs: 500 });
  await queue.push("e");
  await queue.push("f");
  await delay(3000);
  // error
  assertEquals(results, ["b", "a", "c", "d", "c"]);
  assertEquals(errors, ["error", "error", "error", "error"]);
  await delay(1000);
  // e
  assertEquals(results, ["b", "a", "c", "d", "c", "e"]);
  assertEquals(errors, ["error", "error", "error", "error"]);
  controller.abort();
  await delay(1000);
  // error retry
  assertEquals(results, ["b", "a", "c", "d", "c", "e"]);
  assertEquals(errors, ["error", "error", "error", "error", "error"]);
  await delay(1000);
  // nothing because aborted
  assertEquals(results, ["b", "a", "c", "d", "c", "e"]);
  assertEquals(errors, ["error", "error", "error", "error", "error"]);
  await delay(1000);
  const controller2 = new AbortController();
  const processPromise = worker.process(controller2.signal);
  await delay(500);
  // initial after resume
  assertEquals(results, ["b", "a", "c", "d", "c", "e"]);
  assertEquals(errors, ["error", "error", "error", "error", "error"]);
  await delay(1000);
  // f
  assertEquals(results, ["b", "a", "c", "d", "c", "e", "f"]);
  assertEquals(errors, ["error", "error", "error", "error", "error"]);
  await delay(1000);
  // nothing
  assertEquals(results, ["b", "a", "c", "d", "c", "e", "f"]);
  assertEquals(errors, ["error", "error", "error", "error", "error"]);

  controller2.abort();
  await processPromise;
});
