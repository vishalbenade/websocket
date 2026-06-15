// ─────────────────────────────────────────────────────────────
//  logging/stream-logger.ts
//
//  Thin adapter over your @your-org/logger LoggerService, scoped
//  per-class via forContext(). All streaming files inject
//  STREAM_LOGGER_FACTORY (or call this helper) to get a logger
//  pre-tagged with their class name — so every log line is
//  filterable by source (e.g. "MarketDataStreamService",
//  "OptionsStreamService", "RSocketClientFactory").
//
//  ── ASSUMED @your-org/logger API ────────────────────────────────
//  If your actual LoggerService has a different shape, adjust ONLY
//  this file — every other file in core/streaming/ goes through
//  `createStreamLogger()` and is unaffected.
//
//    LoggerService.forContext(context: string): ContextLogger
//    ContextLogger.debug(message: string, meta?: Record<string, unknown>): void
//    ContextLogger.info (message: string, meta?: Record<string, unknown>): void
//    ContextLogger.warn (message: string, meta?: Record<string, unknown>): void
//    ContextLogger.error(message: string, meta?: Record<string, unknown>): void
//
//  This matches the LoggerService / LogTransportService design
//  (context-scoped, structured meta objects, runs outside NgZone,
//  PII-safe — avoid logging full payload bodies, only counts/ids).
// ─────────────────────────────────────────────────────────────

import { inject } from '@angular/core';
import { LoggerService } from '@your-org/logger';

export interface ContextLogger {
  debug(message: string, meta?: Record<string, unknown>): void;
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, meta?: Record<string, unknown>): void;
}

/**
 * Creates a context-scoped logger for the calling class.
 * Call once per service, store as `private readonly logger`.
 *
 * Usage:
 *   private readonly logger = createStreamLogger('MarketDataStreamService');
 *   this.logger.info('Connecting', { host, port });
 */
export function createStreamLogger(context: string): ContextLogger {
  const loggerService = inject(LoggerService);
  return loggerService.forContext(context);
}
