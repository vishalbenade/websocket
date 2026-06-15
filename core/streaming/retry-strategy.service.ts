// ─────────────────────────────────────────────────────────────
//  retry-strategy.service.ts
//  Exponential backoff with jitter. Injected into BaseStreamService.
//  Tested independently of any stream.
// ─────────────────────────────────────────────────────────────

import { Injectable } from '@angular/core';
import { Observable, timer } from 'rxjs';
import { mergeMap, retryWhen, scan } from 'rxjs/operators';
import { createStreamLogger, ContextLogger } from './logging/stream-logger';

export interface RetryOptions {
  maxRetries: number;
  initialDelayMs: number;
  maxDelayMs: number;
  /** Called before each retry attempt with the attempt index (1-based) */
  onRetry: (attempt: number) => void;
}

@Injectable({ providedIn: 'root' })
export class RetryStrategyService {

  private readonly logger: ContextLogger = createStreamLogger('RetryStrategyService');

  /**
   * Returns an RxJS pipeable operator that retries the source observable
   * using exponential backoff + ±25% random jitter.
   *
   * Throws the original error once maxRetries is exceeded so the
   * downstream catchError in BaseStreamService can handle it.
   */
  build(options: RetryOptions) {
    return retryWhen((errors$: Observable<Error>) =>
      errors$.pipe(
        scan((attempt, error) => {
          if (attempt >= options.maxRetries) {
            this.logger.error('Max retry attempts exceeded — giving up', {
              maxRetries: options.maxRetries,
              lastError: error.message,
            });
            throw error;
          }
          return attempt + 1;
        }, 0),
        mergeMap((attempt) => {
          const base  = Math.min(
            options.initialDelayMs * Math.pow(2, attempt - 1),
            options.maxDelayMs
          );
          // ±25% jitter prevents thundering-herd on mass reconnect
          const jitter = base * 0.25 * (Math.random() * 2 - 1);
          const delay  = Math.max(0, Math.floor(base + jitter));

          this.logger.debug('Scheduling retry', {
            attempt,
            delayMs: delay,
            maxRetries: options.maxRetries,
          });

          options.onRetry(attempt);
          return timer(delay);
        })
      )
    );
  }
}
