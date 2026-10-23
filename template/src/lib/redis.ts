import { Redis, type RedisOptions } from 'ioredis';
import type { Logger } from './logger.js';

/** Convert a redis:// or rediss:// URL into BullMQ connection options. */
export function connectionFromUrl(url: string, opts: { forWorker?: boolean } = {}): RedisOptions {
  const u = new URL(url);
  if (u.protocol !== 'redis:' && u.protocol !== 'rediss:') throw new Error(`Unsupported Redis protocol: ${u.protocol}`);
  const db = u.pathname.length > 1 ? Number(u.pathname.slice(1)) : 0;
  return {
    host: u.hostname,
    port: u.port ? Number(u.port) : 6379,
    username: u.username ? decodeURIComponent(u.username) : undefined,
    password: u.password ? decodeURIComponent(u.password) : undefined,
    db: Number.isFinite(db) ? db : 0,
    ...(u.protocol === 'rediss:' ? { tls: {} } : {}),
    // Workers must block forever on Redis (BullMQ requirement); producers should fail fast instead.
    maxRetriesPerRequest: opts.forWorker ? null : 3,
  };
}

/** Log Redis connection errors without repeating them on every reconnect attempt. */
export function attachRedisErrorListener(
  client: Pick<Redis, 'on'>,
  logger: Pick<Logger, 'warn'>,
  intervalMs = 10_000,
): void {
  let lastLoggedAt = Number.NEGATIVE_INFINITY;
  client.on('error', (err: Error & { code?: string }) => {
    const now = Date.now();
    if (now - lastLoggedAt < intervalMs) return;
    lastLoggedAt = now;
    logger.warn({ error: err.message, code: err.code }, 'Redis connection failed; retrying');
  });
}

/**
 * BullMQ in native ESM needs a constructed client instead of plain options.
 * Each Queue/Worker/QueueEvents should get its own client.
 */
export function createRedis(
  url: string,
  logger: Pick<Logger, 'warn'>,
  opts: { forWorker?: boolean } = {},
): Redis {
  const client = new Redis(connectionFromUrl(url, opts));
  attachRedisErrorListener(client, logger);
  return client;
}
