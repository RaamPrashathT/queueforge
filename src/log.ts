import { pino } from 'pino';
import { loadConfig } from './config.js';

export function createLogger(bindings: Record<string, unknown> = {}) {
  const { logLevel } = loadConfig();
  return pino({ level: logLevel }).child(bindings);
}

export type Logger = ReturnType<typeof createLogger>;
