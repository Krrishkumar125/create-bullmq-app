import { serve } from '@hono/node-server';
import { createApp } from './app.js';
import { loadConfig } from './config.js';
import { createLogger } from './lib/logger.js';
import { onShutdown } from './lib/shutdown.js';
import { createQueues } from './queues.js';

const config = loadConfig();
const logger = createLogger(config, 'api');
const { queues, closeAll } = createQueues(config, logger);
let draining = false;

const app = createApp({ queues, logger, isShuttingDown: () => draining, adminToken: config.ADMIN_TOKEN });
const server = serve({ fetch: app.fetch, port: config.PORT }, (info) => logger.info({ port: info.port }, 'api listening'));

onShutdown(
  [
    { name: 'readiness', close: async () => { draining = true; } },
    { name: 'http', close: () => new Promise<void>((res, rej) => server.close((e) => (e ? rej(e) : res()))) },
    { name: 'queues+redis', close: closeAll },
  ],
  { logger, timeoutMs: config.SHUTDOWN_TIMEOUT_MS },
);
