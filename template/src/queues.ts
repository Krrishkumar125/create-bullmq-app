import { Queue } from 'bullmq';
import type { Config } from './config.js';
import { jobs } from './jobs/index.js';
import type { Redis } from 'ioredis';
import type { Logger } from './lib/logger.js';
import { createRedis } from './lib/redis.js';

export function createQueues(config: Config, logger: Logger) {
  const queues = new Map<string, Queue>();
  const clients: Redis[] = [];
  for (const name of Object.keys(jobs)) {
    queues.set(
      name,
      new Queue(name, {
        connection: track(clients, createRedis(config.REDIS_URL, logger)),
        prefix: config.QUEUE_PREFIX,
        defaultJobOptions: {
          attempts: config.JOB_ATTEMPTS,
          backoff: { type: 'exponential', delay: config.JOB_BACKOFF_MS },
          // Bound Redis memory: keep recent history only.
          removeOnComplete: { age: 24 * 3600, count: 1000 },
          removeOnFail: { age: 7 * 24 * 3600, count: 5000 },
        },
      }),
    );
  }
  /** BullMQ does not close clients it did not create, so we do. */
  const closeAll = async () => {
    await Promise.all([...queues.values()].map((q) => q.close()));
    await Promise.all(clients.map((c) => c.quit()));
  };
  return { queues, closeAll };
}

function track(list: Redis[], c: Redis) {
  list.push(c);
  return c;
}
