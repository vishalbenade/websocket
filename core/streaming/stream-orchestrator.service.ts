// ─────────────────────────────────────────────────────────────
//  stream-orchestrator.service.ts
//
//  THIS IS WHERE THE "ONE CONNECTION PER SERVICE, AT APP STARTUP"
//  REQUIREMENT IS WIRED UP.
//
//  connectAll() calls connect() once per stream service — each
//  opens exactly ONE RSocket connection to its host:port. Call this
//  once during app bootstrap (see app.config.ts / APP_INITIALIZER
//  example in README).
//
//  Feature components NEVER call connect()/disconnect() themselves.
//  They call subscribeRoute() / ticksFor() / chainFor() etc., which
//  open/close individual routes on these already-open connections.
//
//  Also provides global health rollup signals for a connection
//  status banner.
// ─────────────────────────────────────────────────────────────

import { Injectable, OnDestroy, inject, computed } from '@angular/core';

import { MarketDataStreamService } from './streams/market-data-stream.service';
import { OptionsStreamService }    from './streams/options-stream.service';
// import { OrderStreamService }    from './streams/order-stream.service';
// import { PositionStreamService } from './streams/position-stream.service';
// import { RiskStreamService }     from './streams/risk-stream.service';

@Injectable({ providedIn: 'root' })
export class StreamOrchestratorService implements OnDestroy {

  // Register every domain service here — one entry per microservice
  // connection. Each entry corresponds to exactly ONE socket.
  private readonly streams = [
    inject(MarketDataStreamService),
    inject(OptionsStreamService),
    // inject(OrderStreamService),
    // inject(PositionStreamService),
    // inject(RiskStreamService),
  ] as const;

  // ── Aggregated computed signals ───────────────────────────────

  /** True when every stream is connected */
  readonly allLive = computed(() =>
    this.streams.every((s) => s.isLive())
  );

  /** True when at least one stream is in an error state */
  readonly anyError = computed(() =>
    this.streams.some((s) => s.error() !== null)
  );

  /** True when at least one connected stream has gone stale */
  readonly anyStale = computed(() =>
    this.streams.some((s) => s.isStale())
  );

  /** True when any stream is attempting to reconnect */
  readonly anyReconnecting = computed(() =>
    this.streams.some((s) => s.status() === 'reconnecting')
  );

  /** Aggregate status for a global health badge */
  readonly globalStatus = computed((): 'healthy' | 'degraded' | 'down' => {
    if (this.anyError())       return 'down';
    if (this.anyStale())       return 'degraded';
    if (!this.allLive())       return 'degraded';
    return 'healthy';
  });

  // ── Bulk actions ──────────────────────────────────────────────

  /**
   * Opens ONE RSocket connection per registered service.
   * Call once at app startup — see README for an APP_INITIALIZER
   * example. Idempotent: each service's connect() is a no-op if
   * already connected.
   */
  connectAll(): void {
    this.streams.forEach((s) => s.connect());
  }

  /**
   * Closes all connections and cancels all active routes across
   * all services. Typically called on logout.
   */
  disconnectAll(): void {
    this.streams.forEach((s) => s.disconnect());
  }

  ngOnDestroy(): void {
    this.disconnectAll();
  }
}
