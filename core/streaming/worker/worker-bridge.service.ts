// ─────────────────────────────────────────────────────────────
//  worker-bridge.service.ts
//  Main-thread interface to computation.worker.ts.
//
//  • Spawns a single shared worker instance (providedIn: root)
//  • Correlates requests ↔ responses via UUID map
//  • Exposes run<TIn, TOut>() → Promise<TOut>
//  • Use from(workerBridge.run(...)) for RxJS integration
// ─────────────────────────────────────────────────────────────

import { Injectable, OnDestroy } from '@angular/core';
import { WorkerRequest, WorkerResponse, WorkerTask } from './worker-bridge.types';
import { createStreamLogger, ContextLogger } from '../logging/stream-logger';

interface PendingHandler {
  resolve: (value: unknown) => void;
  reject:  (error: Error)   => void;
  task: WorkerTask;
  startedAt: number;
}

@Injectable({ providedIn: 'root' })
export class WorkerBridgeService implements OnDestroy {

  private readonly logger: ContextLogger = createStreamLogger('WorkerBridgeService');
  private readonly worker: Worker;

  /**
   * Pending promise handlers keyed by request UUID.
   * Cleared on resolution, rejection, or service destroy.
   */
  private readonly pending = new Map<string, PendingHandler>();

  constructor() {
    // Angular CLI resolves this URL at build time and code-splits
    // the worker into a separate bundle automatically.
    this.worker = new Worker(
      new URL('./computation.worker', import.meta.url),
      { type: 'module' }
    );

    this.logger.info('Computation worker spawned');

    this.worker.onmessage = ({ data }: MessageEvent<WorkerResponse>) => {
      const handler = this.pending.get(data.id);
      if (!handler) {
        this.logger.warn('Received worker response for unknown request id', { id: data.id });
        return;
      }

      this.pending.delete(data.id);
      const durationMs = Date.now() - handler.startedAt;

      if (data.error) {
        this.logger.error('Worker task failed', {
          task: handler.task,
          id: data.id,
          durationMs,
          error: data.error,
        });
        handler.reject(new Error(data.error));
      } else {
        this.logger.debug('Worker task completed', {
          task: handler.task,
          id: data.id,
          durationMs,
        });
        handler.resolve(data.result);
      }
    };

    this.worker.onerror = (event: ErrorEvent) => {
      this.logger.error('Uncaught worker error — rejecting all pending tasks', {
        message: event.message,
        pendingCount: this.pending.size,
      });
      // Reject all pending requests so callers don't hang indefinitely
      this.pending.forEach(({ reject }) =>
        reject(new Error(`Worker crashed: ${event.message}`))
      );
      this.pending.clear();
    };
  }

  /**
   * Sends a task to the worker and returns a typed Promise.
   *
   * For RxJS integration inside a stream service's route wrapper method:
   *   mergeMap(raw => from(this.worker.run('PARSE_ORDERBOOK', raw)))
   *
   * @param task     One of the WorkerTask union values
   * @param payload  Task-specific input (see worker-bridge.types.ts)
   */
  run<TIn, TOut>(task: WorkerTask, payload: TIn): Promise<TOut> {
    return new Promise<TOut>((resolve, reject) => {
      const id = crypto.randomUUID();

      this.logger.debug('Dispatching worker task', { task, id });

      this.pending.set(id, {
        resolve: resolve as (v: unknown) => void,
        reject,
        task,
        startedAt: Date.now(),
      });

      this.worker.postMessage({ id, task, payload } satisfies WorkerRequest);
    });
  }

  ngOnDestroy(): void {
    this.logger.info('Terminating computation worker', { pendingCount: this.pending.size });
    this.worker.terminate();
    this.pending.forEach(({ reject }) =>
      reject(new Error('WorkerBridgeService destroyed'))
    );
    this.pending.clear();
  }
}
