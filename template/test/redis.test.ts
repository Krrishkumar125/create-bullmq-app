import { EventEmitter } from 'node:events';
import { describe, expect, it, vi } from 'vitest';
import { attachRedisErrorListener, connectionFromUrl } from '../src/lib/redis.js';

describe('connectionFromUrl', () => {
  it('parses host, port, auth and db', () => {
    expect(connectionFromUrl('redis://user:p%40ss@cache:6380/2')).toMatchObject({
      host: 'cache', port: 6380, username: 'user', password: 'p@ss', db: 2, maxRetriesPerRequest: 3,
    });
  });
  it('enables TLS for rediss://', () => {
    expect(connectionFromUrl('rediss://h')).toMatchObject({ tls: {}, port: 6379 });
  });
  it('uses blocking-safe settings for workers', () => {
    expect(connectionFromUrl('redis://h', { forWorker: true })).toMatchObject({ maxRetriesPerRequest: null });
  });
  it('rejects other protocols', () => {
    expect(() => connectionFromUrl('http://h')).toThrow(/Unsupported/);
  });
});

describe('attachRedisErrorListener', () => {
  it('logs a concise warning at most once per 10 seconds', () => {
    vi.useFakeTimers();
    try {
      const client = new EventEmitter();
      const warn = vi.fn();
      attachRedisErrorListener(client as never, { warn });
      const first = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' });

      client.emit('error', first);
      client.emit('error', first);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(warn).toHaveBeenCalledWith(
        { error: 'connect ECONNREFUSED', code: 'ECONNREFUSED' },
        'Redis connection failed; retrying',
      );

      vi.advanceTimersByTime(9_999);
      client.emit('error', first);
      expect(warn).toHaveBeenCalledTimes(1);

      vi.advanceTimersByTime(1);
      client.emit('error', first);
      expect(warn).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });
});
