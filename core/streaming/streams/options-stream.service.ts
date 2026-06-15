// ─────────────────────────────────────────────────────────────
//  streams/options-stream.service.ts
//  ONE shared connection to the options microservice.
//  connect() should be called ONCE at app startup.
//
//  Feature components call chainFor(underlyingSymbol, subscriberId) —
//  each underlying opens its own RSocket route (e.g.
//  'options.chain.SPY') over the SAME connection. Raw frames are
//  parsed synchronously by parseFrame() (base class requirement),
//  then enriched with Greeks via WorkerBridgeService (mergeMap) in
//  chainFor() — the worker hop is invisible to BaseStreamService.
//
//  Route convention: 'options.chain.<UNDERLYING_SYMBOL>'
// ─────────────────────────────────────────────────────────────

import { Injectable, inject } from '@angular/core';
import { Observable, from }   from 'rxjs';
import { mergeMap }           from 'rxjs/operators';

import { BaseStreamService }   from '../base-stream.service';
import { StreamConfig }        from '../stream.types';
import { WorkerBridgeService } from '../worker/worker-bridge.service';
import { Greeks, GreeksInput } from '../worker/worker-bridge.types';

// ── Raw frame shape (as received, pre-worker) ─────────────────

export interface RawOptionFrame {
  symbol:     string;
  strike:     number;
  expiry:     string;
  optionType: 'CALL' | 'PUT';
  bid:        number;
  ask:        number;
  iv:         number;
  openInterest: number;
  underlyingPrice: number;
  riskFreeRate:    number;
  timeToExpiry:    number;
}

// ── Enriched domain model (post-worker) ────────────────────────

export interface OptionChainRow {
  symbol:     string;
  strike:     number;
  expiry:     string;
  optionType: 'CALL' | 'PUT';
  bid:        number;
  ask:        number;
  iv:         number;
  openInterest: number;
  greeks:     Greeks;
}

// ─────────────────────────────────────────────────────────────

@Injectable({ providedIn: 'root' })
export class OptionsStreamService extends BaseStreamService<RawOptionFrame> {

  private readonly workerBridge = inject(WorkerBridgeService);

  constructor() {
    super('OptionsStreamService');
  }

  protected override readonly config: StreamConfig = {
    host:  'options-svc.internal',
    port:  7002,
    idleThresholdMs:  10_000,
    staleThresholdMs: 60_000,
    maxRetries: 10,
  };

  /**
   * Synchronous decode — required by BaseStreamService for every
   * route on this connection. Greeks enrichment (CPU-heavy, worker-
   * bound) happens downstream in chainFor(), not here.
   */
  protected override parseFrame(buffer: Buffer): RawOptionFrame {
    return JSON.parse(buffer.toString()) as RawOptionFrame;
  }

  // ── Convenience selectors ─────────────────────────────────────

  /**
   * Subscribes to the real-time option chain for an underlying symbol,
   * with Greeks calculated off the main thread via WorkerBridgeService.
   *
   * Opens (or reuses) the RSocket route 'options.chain.<symbol>' on the
   * shared connection. Multiple components calling this with the same
   * underlying but different subscriberIds share one underlying stream
   * — but each gets its OWN worker enrichment pipeline (mergeMap is
   * per-subscription, lightweight).
   *
   * @param underlyingSymbol  e.g. 'SPY'
   * @param subscriberId      Unique ID for this subscriber (component UUID)
   */
  chainFor(underlyingSymbol: string, subscriberId: string): Observable<OptionChainRow> {
    return this.subscribeRoute(`options.chain.${underlyingSymbol}`, subscriberId).pipe(
      mergeMap((frame) =>
        from(
          this.workerBridge
            .run<GreeksInput, Greeks>('CALC_GREEKS', {
              S:     frame.underlyingPrice,
              K:     frame.strike,
              T:     frame.timeToExpiry,
              r:     frame.riskFreeRate,
              sigma: frame.iv,
            })
            .then((greeks): OptionChainRow => ({
              symbol:       frame.symbol,
              strike:       frame.strike,
              expiry:       frame.expiry,
              optionType:   frame.optionType,
              bid:          frame.bid,
              ask:          frame.ask,
              iv:           frame.iv,
              openInterest: frame.openInterest,
              greeks,
            }))
            .catch((err: Error) => {
              this.logger.error('Greeks worker computation failed', {
                underlying: underlyingSymbol,
                symbol: frame.symbol,
                error: err.message,
              });
              throw err;
            })
        )
      )
    );
  }

  /**
   * Stops receiving the option chain for an underlying for this
   * subscriber. If no other subscriber is watching, the underlying
   * RSocket route is cancelled (the connection itself stays open).
   */
  unsubscribeChain(underlyingSymbol: string, subscriberId: string): void {
    this.unsubscribe(`options.chain.${underlyingSymbol}`, subscriberId);
  }
}
