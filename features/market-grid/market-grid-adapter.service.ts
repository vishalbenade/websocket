// ─────────────────────────────────────────────────────────────
//  market-grid-adapter.service.ts
//  Bridges MarketDataStreamService (reactive, multi-route) → AG Grid
//  (imperative).
//
//  IMPORTANT — connection lifecycle changed:
//  MarketDataStreamService.connect() is called ONCE at app startup
//  (see StreamOrchestratorService.connectAll()). This adapter does
//  NOT call connect()/disconnect() — it only opens/closes individual
//  SYMBOL ROUTES on the already-open shared connection.
//
//  Two-stage batching strategy:
//    Stage 1 — bufferTime(N ms): collapses rapid ticks into windows
//    Stage 2 — applyTransactionAsync: AG Grid's internal flush queue
//
//  Provided at component level (providers: [MarketGridAdapterService])
//  so its lifecycle is tied to the grid component.
// ─────────────────────────────────────────────────────────────

import { Injectable, OnDestroy, inject } from '@angular/core';
import { Subject, Subscription, merge }   from 'rxjs';
import { bufferTime, filter, takeUntil }  from 'rxjs/operators';
import { GridApi, RowDataTransaction }    from 'ag-grid-community';

import { MarketDataStreamService, MarketTick } from
  '../../core/streaming/streams/market-data-stream.service';
import { createStreamLogger, ContextLogger } from
  '../../core/streaming/logging/stream-logger';

@Injectable()
export class MarketGridAdapterService implements OnDestroy {

  private readonly streamSvc = inject(MarketDataStreamService);
  private readonly logger: ContextLogger = createStreamLogger('MarketGridAdapterService');
  private readonly destroy$  = new Subject<void>();

  private gridApi?: GridApi<MarketTick>;
  private streamSub?: Subscription;

  /** Unique ID for this adapter instance — used as the subscriberId
   *  across all symbol routes it opens. */
  private readonly subscriberId = `market-grid-adapter-${crypto.randomUUID()}`;

  /** Symbols currently subscribed via this adapter */
  private activeSymbols: string[] = [];

  // Buffer window in ms — tune to match tick frequency
  // See comment block in market-grid.component.ts for tuning guide
  private readonly BUFFER_WINDOW_MS = 100;

  // ── Public API ────────────────────────────────────────────────

  /**
   * Attach to an initialised AG Grid API and start consuming ticks
   * for the given symbols. Must be called inside the (gridReady)
   * event handler to ensure the row model exists before any
   * transactions are applied.
   *
   * The underlying connection is assumed to already be open
   * (established once at app startup) — this method only opens
   * per-symbol routes on it.
   *
   * @param api      AG Grid API from the gridReady event
   * @param symbols  Instrument symbols to stream, e.g. ['AAPL','MSFT']
   */
  attach(api: GridApi<MarketTick>, symbols: string[]): void {
    if (this.gridApi) {
      this.logger.warn('attach() called but adapter already attached — use updateSymbols() instead', {
        subscriberId: this.subscriberId,
        currentSymbols: this.activeSymbols,
      });
      return;
    }

    this.logger.info('Attaching grid adapter', {
      subscriberId: this.subscriberId,
      symbols,
    });

    this.gridApi = api;
    this.subscribeToSymbols(symbols);
  }

  /**
   * Changes the set of symbols this grid streams, without detaching.
   * Useful for e.g. a watchlist where the user adds/removes rows.
   *
   * Diffs against the currently active symbols: only newly added
   * symbols open new routes, and only removed symbols are unsubscribed
   * (other components watching the same symbol are unaffected).
   */
  updateSymbols(symbols: string[]): void {
    const next = new Set(symbols);
    const prev = new Set(this.activeSymbols);

    const added   = symbols.filter((s) => !prev.has(s));
    const removed = this.activeSymbols.filter((s) => !next.has(s));

    if (added.length === 0 && removed.length === 0) {
      this.logger.debug('updateSymbols() called with no effective change', { symbols });
      return;
    }

    this.logger.info('Updating watched symbols', {
      subscriberId: this.subscriberId,
      added,
      removed,
    });

    removed.forEach((symbol) =>
      this.streamSvc.unsubscribeTicks(symbol, this.subscriberId)
    );

    this.streamSub?.unsubscribe();
    this.subscribeToSymbols(symbols);
  }

  /**
   * Detach from the grid and stop consuming all symbol routes.
   * The shared RSocket connection remains open — other components
   * may still be using it for other symbols/streams.
   */
  detach(): void {
    this.logger.info('Detaching grid adapter', {
      subscriberId: this.subscriberId,
      symbols: this.activeSymbols,
    });

    this.streamSub?.unsubscribe();
    this.activeSymbols.forEach((symbol) =>
      this.streamSvc.unsubscribeTicks(symbol, this.subscriberId)
    );
    this.activeSymbols = [];
    this.gridApi = undefined;
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.detach();
  }

  // ── Private ───────────────────────────────────────────────────

  private subscribeToSymbols(symbols: string[]): void {
    this.activeSymbols = [...symbols];

    if (symbols.length === 0) {
      this.logger.debug('No symbols to subscribe — skipping route setup');
      return;
    }

    this.logger.debug('Opening symbol routes', {
      subscriberId: this.subscriberId,
      symbols,
      bufferWindowMs: this.BUFFER_WINDOW_MS,
    });

    // Each symbol is its own RSocket route on the shared connection.
    // merge() fans them all into one stream for batching.
    const ticks$ = merge(
      ...symbols.map((symbol) => this.streamSvc.ticksFor(symbol, this.subscriberId))
    );

    this.streamSub = ticks$
      .pipe(
        // Collect all ticks within the buffer window into an array
        bufferTime(this.BUFFER_WINDOW_MS),
        // Skip empty windows (no ticks arrived)
        filter((ticks) => ticks.length > 0),
        takeUntil(this.destroy$)
      )
      .subscribe((ticks) => this.flushToGrid(ticks));
  }

  /**
   * Flush a batch of ticks to AG Grid as a single transaction.
   *
   * Deduplication: within one buffer window, multiple ticks for the
   * same symbol are collapsed to the latest — preventing redundant
   * row updates that would cause unnecessary cell flashes.
   */
  private flushToGrid(ticks: MarketTick[]): void {
    if (!this.gridApi) return;

    // Keep only the last tick per symbol within this buffer window
    const latestBySymbol = new Map<string, MarketTick>();
    for (const tick of ticks) {
      latestBySymbol.set(tick.symbol, tick);
    }

    const transaction: RowDataTransaction<MarketTick> = {
      update: [...latestBySymbol.values()],
    };

    // applyTransactionAsync is AG Grid's high-frequency update API.
    // It queues transactions and flushes them on the next animation
    // frame in batches, preventing layout thrashing.
    this.gridApi.applyTransactionAsync(transaction, (result) => {
      // Callback fires after AG Grid processes the transaction.
      // Rows not found by getRowId are silently skipped — this is
      // expected for new symbols not yet in the grid's row data.
      if (result.update.length === 0 && ticks.length > 0) {
        this.logger.debug('No existing rows matched — adding as new rows', {
          symbols: [...latestBySymbol.keys()],
        });
        // All rows were new — add them instead
        this.gridApi?.applyTransactionAsync({ add: [...latestBySymbol.values()] });
      }
    });
  }
}
