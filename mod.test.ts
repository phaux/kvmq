import { assertEquals } from "https://deno.land/std@0.203.0/assert/assert_equals.ts";
import { assertObjectMatch } from "https://deno.land/std@0.203.0/assert/assert_object_match.ts";
import { delay } from "https://deno.land/std@0.201.0/async/delay.ts";
import { handlers, setup } from "https://deno.land/std@0.201.0/log/mod.ts";
import { Queue } from "./mod.ts";

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

  await queue.pushJob("a");
  await queue.pushJob("b", { priority: 1 });
  await queue.pushJob("error", { retryCount: 1 });
  await queue.pushJob("c", { repeatCount: 1 });
  await queue.pushJob("error");
  await queue.pushJob("d");

  const jobs = await queue.getAllJobs();

  assertEquals(jobs.length, 6);

  assertObjectMatch(jobs[0], { state: "b", place: 1, status: "waiting" });
  assertObjectMatch(jobs[1], { state: "a", place: 2, status: "waiting" });
  assertObjectMatch(jobs[2], { state: "error", place: 3, status: "waiting" });

  const results: string[] = [];
  const errors: string[] = [];

  const worker = queue.createWorker(async (job) => {
    console.log("processing", job.state);
    await delay(1000);
    if (job.state === "error") {
      throw new Error("error");
    }
    results.push(job.state);
  });

  worker.addEventListener("error", (e) => {
    errors.push(e.detail.error.message);
  });

  const controller = new AbortController();
  worker.processJobs({ signal: controller.signal });

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
  await queue.pushJob("error", { retryCount: 1, retryDelayMs: 500 });
  await queue.pushJob("e");
  await queue.pushJob("f");
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
  const processPromise = worker.processJobs({ signal: controller2.signal });
  await delay(500);
  // initial after resume
  assertEquals(results, ["b", "a", "c", "d", "c", "e"]);
  assertEquals(errors, ["error", "error", "error", "error", "error"]);
  const jobs2 = await queue.getAllJobs();
  assertEquals(jobs2.length, 1);
  assertObjectMatch(jobs2[0], { state: "f", place: 0, status: "processing" });
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
