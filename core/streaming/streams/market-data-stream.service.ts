// ─────────────────────────────────────────────────────────────
//  streams/market-data-stream.service.ts
//  ONE shared connection to the market-data microservice.
//  connect() should be called ONCE at app startup (see
//  StreamOrchestratorService.connectAll()).
//
//  Feature components then call ticksFor(symbol, subscriberId) as
//  many times as needed — each symbol opens its own RSocket route
//  (e.g. 'market.ticks.AAPL') over the SAME connection, and multiple
//  components can subscribe to the same symbol independently.
//
//  Route convention: 'market.ticks.<SYMBOL>'
// ─────────────────────────────────────────────────────────────

import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

import { BaseStreamService } from '../base-stream.service';
import { StreamConfig }      from '../stream.types';

// ── Domain model ──────────────────────────────────────────────
export interface MarketTick {
  symbol:    string;
  bid:       number;
  ask:       number;
  bidSize:   number;
  askSize:   number;
  timestamp: number;   // Unix ms
}

// ─────────────────────────────────────────────────────────────

@Injectable({ providedIn: 'root' })
export class MarketDataStreamService extends BaseStreamService<MarketTick> {

  constructor() {
    super('MarketDataStreamService');
  }

  protected override readonly config: StreamConfig = {
    host:  'market-svc.internal',
    port:  7000,
    // High-frequency: declare idle fast so stale alerts fire promptly
    idleThresholdMs:  2_000,
    staleThresholdMs: 10_000,
    maxRetries:       15,
    initialRetryDelayMs: 500,
    maxRetryDelayMs:  20_000,
  };

  protected override parseFrame(buffer: Buffer): MarketTick {
    return JSON.parse(buffer.toString()) as MarketTick;
  }

  // ── Convenience selectors ─────────────────────────────────────

  /**
   * Subscribes to real-time ticks for a single symbol.
   *
   * Opens (or reuses) the RSocket route 'market.ticks.<symbol>' on the
   * shared connection. Multiple components calling this with the same
   * symbol but different subscriberIds share one underlying stream.
   *
   * @param symbol        Instrument symbol, e.g. 'AAPL'
   * @param subscriberId  Unique ID for this subscriber (e.g. component UUID)
   */
  ticksFor(symbol: string, subscriberId: string): Observable<MarketTick> {
    return this.subscribeRoute(`market.ticks.${symbol}`, subscriberId);
  }

  /**
   * Stops receiving ticks for a symbol for this subscriber.
   * If no other subscriber is watching this symbol, the underlying
   * RSocket route is cancelled (the connection itself stays open).
   */
  unsubscribeTicks(symbol: string, subscriberId: string): void {
    this.unsubscribe(`market.ticks.${symbol}`, subscriberId);
  }
}
