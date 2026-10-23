import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { Queue, QueueEvents, Worker } from 'bullmq';
import { createRedis } from '../src/lib/redis.js';
import * as email from '../src/jobs/email.js';

// Runs only when a real Redis is available (CI provides one).
const url = process.env.REDIS_URL;

describe.skipIf(!url)('integration (real Redis)', () => {
  const prefix = `test-${Date.now()}`;
  let queue: Queue;
  let events: QueueEvents;
  let worker: Worker;
  const clients: ReturnType<typeof createRedis>[] = [];
  const logger = { warn: () => {} };
  const client = (forWorker = false) => { const c = createRedis(url!, logger, { forWorker }); clients.push(c); return c; };

  beforeAll(async () => {
    queue = new Queue(email.name, { connection: client(), prefix });
    events = new QueueEvents(email.name, { connection: client(true), prefix });
    worker = new Worker(email.name, email.process, { connection: client(true), prefix });
    await Promise.all([queue.waitUntilReady(), events.waitUntilReady(), worker.waitUntilReady()]);
  });

  afterAll(async () => {
    await worker?.close();
    await queue?.obliterate({ force: true });
    await Promise.all([queue?.close(), events?.close()]);
    await Promise.all(clients.map((c) => c.quit()));
  });

  it('processes a job end to end', async () => {
    const job = await queue.add(email.name, { to: 'a@b.co', subject: 's', body: 'b' });
    await expect(job.waitUntilFinished(events, 10_000)).resolves.toEqual({ delivered: true, to: 'a@b.co' });
  });

  it('deduplicates by idempotency key', async () => {
    const p = { to: 'a@b.co', subject: 's', body: 'b', idempotencyKey: 'same' };
    const a = await queue.add(email.name, p, { jobId: email.jobId(p) });
    const b = await queue.add(email.name, p, { jobId: email.jobId(p) });
    expect(a.id).toBe(b.id);
  });

  it('upserting a schedule twice keeps a single scheduler', async () => {
    await queue.upsertJobScheduler('t-schedule', { every: 60_000 }, { name: email.name, data: { to: 'a@b.co', subject: 's', body: 'b' } });
    await queue.upsertJobScheduler('t-schedule', { every: 60_000 }, { name: email.name, data: { to: 'a@b.co', subject: 's', body: 'b' } });
    expect(await queue.getJobSchedulersCount()).toBe(1);
    await queue.removeJobScheduler('t-schedule');
  });

  it('worker close drains and stops', async () => {
    await worker.close();
    expect(worker.isRunning()).toBe(false);
  });
});
