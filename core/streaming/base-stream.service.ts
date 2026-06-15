// ─────────────────────────────────────────────────────────────
//  base-stream.service.ts
//  Abstract base for all domain stream services.
//
//  ─── ARCHITECTURE: ONE CONNECTION, MANY ROUTES ─────────────────
//
//  Each concrete service (MarketDataStreamService, OptionsStreamService,
//  etc.) represents ONE host:port — i.e. ONE microservice — and owns
//  exactly ONE RSocket connection, established once via connect()
//  (typically at app startup) and reused for the lifetime of the app.
//
//  On top of that single connection, feature components can open and
//  close any number of independent RSocket requestStream subscriptions
//  — one per "route" (e.g. 'market.ticks.AAPL', 'market.ticks.MSFT',
//  'options.chain.SPY') — via subscribeRoute(). Each route subscription
//  is independent: opening/closing one route does not affect others or
//  the underlying socket connection.
//
//  Responsibilities:
//    • Socket-level connection lifecycle (connect / disconnect) — ONCE
//    • Per-route, per-subscriber requestStream subscriptions
//      (subscribeRoute / unsubscribe)
//    • Exponential backoff retry for the SOCKET connection
//    • Signal-based connection state (status, activity, lastMessageAt, error)
//    • Activity timer: connected → idle → stale (across all routes)
//
//  Subclasses only need to:
//    1. Declare `config` (StreamConfig — host/port/thresholds, no route)
//    2. Implement `parseFrame(buffer)` → T   (route-agnostic frame decode)
//    3. Pass a context name to super() for logging, e.g.
//       super('MarketDataStreamService')
//
//  If different routes on the same connection return different payload
//  shapes, make T a union type and have parseFrame discriminate, or
//  have the subclass expose multiple typed wrappers around
//  subscribeRoute() (see OptionsStreamService for an example).
//
//  ─── LOGGING ─────────────────────────────────────────────────────
//  Every state transition, route open/close, and error is logged via
//  the shared ContextLogger (core/streaming/logging/stream-logger.ts),
//  which wraps your @your-org/logger LoggerService. Log levels:
//    debug — high-frequency / per-frame events (route opened, frame
//            counts, activity timer transitions)
//    info  — lifecycle milestones (connect/disconnect, route
//            subscribe/unsubscribe, reconnection success)
//    warn  — recoverable issues (retry attempts, route-level errors
//            that don't take down the connection)
//    error — terminal failures (max retries exceeded, parse errors)
// ─────────────────────────────────────────────────────────────

import { OnDestroy, signal, computed } from '@angular/core';
import { Observable, Subject, Subscription, EMPTY, ReplaySubject } from 'rxjs';
import { catchError, takeUntil, finalize } from 'rxjs/operators';
import { RSocket, Requestable, Cancellable } from 'rsocket-core';
import {
  encodeCompositeMetadata,
  encodeRoute,
  WellKnownMimeType,
} from 'rsocket-composite-metadata';

import { RSocketClientFactory }                          from './rsocket-client.factory';
import { RetryStrategyService }                          from './retry-strategy.service';
import { StreamConfig, StreamState, INITIAL_STREAM_STATE } from './stream.types';
import { createStreamLogger, ContextLogger }             from './logging/stream-logger';
import { inject } from '@angular/core';

/**
 * Internal bookkeeping for one active route subscription.
 * A route can have multiple named subscribers (subscribe('id1'),
 * subscribe('id2')) sharing the same underlying RSocket requestStream.
 */
interface RouteEntry<T> {
  /** Multicast subject — all subscribers for this route receive from here */
  data$: Subject<T>;
  /** Handle returned by socket.requestStream() — used to cancel */
  requester?: Cancellable & Requestable;
  /** Named subscriber stop-signals, scoped to this route */
  subscribers: Map<string, Subject<void>>;
  /** RxJS subscription driving the requestStream → data$ pipe */
  pipeSub?: Subscription;
  /** Running count of frames received on this route — debug logging only */
  frameCount: number;
}

export abstract class BaseStreamService<T = unknown> implements OnDestroy {

  // ── DI ────────────────────────────────────────────────────────
  private readonly factory   = inject(RSocketClientFactory);
  private readonly retrySvc  = inject(RetryStrategyService);

  /**
   * Context-scoped logger — pass the concrete class name to super()
   * so every log line is tagged with its source service, e.g.
   * "MarketDataStreamService".
   */
  protected readonly logger: ContextLogger;

  constructor(loggerContext: string) {
    this.logger = createStreamLogger(loggerContext);
  }

  // ── Subclass contract ─────────────────────────────────────────
  protected abstract readonly config: StreamConfig;

  /**
   * Decodes a raw RSocket payload Buffer into a domain message.
   * Called for every frame received on every route on this connection.
   *
   * If routes on this connection emit different shapes, parse to a
   * union/discriminated type here, or override per-route by passing
   * a custom parser into subscribeRoute().
   */
  protected abstract parseFrame(buffer: Buffer): T;

  // ── Internal: connection-level ──────────────────────────────────
  private readonly _destroy$ = new Subject<void>();
  private connectionSub?: Subscription;
  private activityTimer?: ReturnType<typeof setTimeout>;

  /**
   * The live RSocket instance, available once connected.
   * Replayed so routes subscribed after connect() still get it.
   */
  private readonly _socket$ = new ReplaySubject<RSocket>(1);

  // ── Internal: per-route bookkeeping ──────────────────────────────
  private readonly _routes = new Map<string, RouteEntry<T>>();

  // ── Thresholds — override per stream ─────────────────────────
  protected get idleThresholdMs():  number { return this.config.idleThresholdMs  ?? 5_000;  }
  protected get staleThresholdMs(): number { return this.config.staleThresholdMs ?? 30_000; }

  // ── Signal state (connection-level) ───────────────────────────
  readonly state = signal<StreamState>({ ...INITIAL_STREAM_STATE });

  readonly status          = computed(() => this.state().status);
  readonly activity        = computed(() => this.state().activity);
  readonly isLive          = computed(() => this.state().status === 'connected');
  readonly isStreaming     = computed(() => this.state().activity === 'active');
  readonly isStale         = computed(() => this.state().activity === 'stale');
  readonly lastMessageAt   = computed(() => this.state().lastMessageAt);
  readonly lastConnectedAt = computed(() => this.state().lastConnectedAt);
  readonly error           = computed(() => this.state().error);
  readonly retryCount      = computed(() => this.state().retryCount);

  // ── Connection API (call ONCE — e.g. at app bootstrap) ─────────

  /**
   * Opens the RSocket connection for this service's host:port.
   * Idempotent — safe to call multiple times; re-connects only if
   * the previous connection is closed.
   *
   * Call this ONCE per service (e.g. in an APP_INITIALIZER or a
   * root "connect all" routine). All subsequent subscribeRoute()
   * calls reuse this single socket.
   */
  connect(): void {
    if (this.connectionSub && !this.connectionSub.closed) {
      this.logger.debug('connect() called but connection already active — ignoring', {
        host: this.config.host,
        port: this.config.port,
      });
      return;
    }

    this.logger.info('Connecting', { host: this.config.host, port: this.config.port });
    this.patch({ status: 'connecting', retryCount: 0, error: null });

    this.connectionSub = this.factory
      .create(this.config.host, this.config.port)
      .pipe(
        this.retrySvc.build({
          maxRetries:     this.config.maxRetries          ?? 10,
          initialDelayMs: this.config.initialRetryDelayMs ?? 1_000,
          maxDelayMs:     this.config.maxRetryDelayMs     ?? 30_000,
          onRetry: (attempt) => {
            this.logger.warn('Retrying connection', {
              attempt,
              host: this.config.host,
              port: this.config.port,
            });
            this.patch({ status: 'reconnecting', retryCount: attempt });
          },
        }),
        catchError((err: Error) => {
          this.logger.error('Connection failed permanently — max retries exceeded', {
            host: this.config.host,
            port: this.config.port,
            error: err.message,
          });
          this.patch({ status: 'error', error: err, activity: 'idle' });
          this.clearActivityTimer();
          return EMPTY;
        }),
        takeUntil(this._destroy$)
      )
      .subscribe({
        next: (socket: RSocket) => {
          const wasReconnect = this.state().retryCount > 0;
          this.logger.info(wasReconnect ? 'Reconnected' : 'Connected', {
            host: this.config.host,
            port: this.config.port,
            activeRoutes: this._routes.size,
          });

          this.patch({ status: 'connected', error: null });
          this._socket$.next(socket);

          // Re-open any routes that were registered before this
          // (re)connection — handles the reconnect-after-drop case.
          this._routes.forEach((entry, route) => this.openRoute(route, entry, socket));
        },
        error: (err: Error) => {
          this.logger.error('Connection error', {
            host: this.config.host,
            port: this.config.port,
            error: err.message,
          });
          this.patch({ status: 'error', error: err, activity: 'idle' });
          this.clearActivityTimer();
        },
        complete: () => {
          this.logger.info('Connection closed', {
            host: this.config.host,
            port: this.config.port,
          });
          this.patch({ status: 'disconnected', activity: 'idle' });
          this.clearActivityTimer();
        },
      });
  }

  /**
   * Closes the RSocket connection entirely. Cancels all active route
   * subscriptions. Route subscriber registrations are preserved —
   * calling connect() again will re-open all previously active routes.
   */
  disconnect(): void {
    this.logger.info('Disconnecting', {
      host: this.config.host,
      port: this.config.port,
      activeRoutes: this._routes.size,
    });

    this.connectionSub?.unsubscribe();
    this.clearActivityTimer();

    // Cancel all active route requesters but keep subscriber maps
    // so reconnect can re-establish them.
    this._routes.forEach((entry, route) => {
      this.logger.debug('Cancelling route requester on disconnect', { route });
      entry.pipeSub?.unsubscribe();
      entry.requester?.cancel();
      entry.requester = undefined;
      entry.pipeSub   = undefined;
    });

    this.patch({ status: 'disconnected', activity: 'idle' });
  }

  // ── Route Subscription API ──────────────────────────────────────

  /**
   * Opens (or reuses) an RSocket requestStream for the given route,
   * over the shared connection, and returns a scoped Observable<T>
   * for this subscriber.
   *
   * • Multiple components can call subscribeRoute() with the SAME
   *   route and DIFFERENT subscriber IDs — they share one underlying
   *   requestStream and each receive every emission.
   * • Calling unsubscribe(route, id) removes only that subscriber.
   *   When the last subscriber for a route unsubscribes, the
   *   underlying requestStream is cancelled automatically.
   * • If connect() hasn't resolved yet, the route is queued and
   *   opened as soon as the socket becomes available.
   *
   * @param route         RSocket route string, e.g. 'market.ticks.AAPL'
   * @param subscriberId  Unique ID for this subscriber (e.g. component UUID)
   */
  subscribeRoute(route: string, subscriberId: string): Observable<T> {
    let entry = this._routes.get(route);

    if (!entry) {
      this.logger.info('Opening new route', { route, subscriberId });

      entry = {
        data$: new Subject<T>(),
        subscribers: new Map<string, Subject<void>>(),
        frameCount: 0,
      };
      this._routes.set(route, entry);

      // If we're already connected, open this route immediately.
      // Otherwise it will be opened by connect()'s `next` handler
      // once the socket becomes available (or on reconnect).
      if (this.isLive()) {
        this._socket$.subscribe((socket) => {
          // Guard: route may have been removed (all unsubscribed)
          // before this async resolution completes.
          const current = this._routes.get(route);
          if (current && !current.requester) {
            this.openRoute(route, current, socket);
          }
        });
      } else {
        this.logger.debug('Connection not yet live — route will open once connected', {
          route,
          status: this.status(),
        });
      }
    } else {
      this.logger.debug('Reusing existing route for new subscriber', {
        route,
        subscriberId,
        existingSubscribers: entry.subscribers.size,
      });
    }

    if (!entry.subscribers.has(subscriberId)) {
      entry.subscribers.set(subscriberId, new Subject<void>());
    }

    return entry.data$.pipe(
      takeUntil(entry.subscribers.get(subscriberId)!),
      finalize(() => this.cleanupSubscriber(route, subscriberId))
    );
  }

  /**
   * Stops message delivery to one subscriber for a given route.
   * If this was the last subscriber for the route, the underlying
   * RSocket requestStream is cancelled and the route entry is removed.
   *
   * @param route         The route previously passed to subscribeRoute()
   * @param subscriberId  The same subscriber ID used in subscribeRoute()
   */
  unsubscribe(route: string, subscriberId: string): void {
    const entry = this._routes.get(route);
    if (!entry) {
      this.logger.debug('unsubscribe() called for unknown route — ignoring', {
        route,
        subscriberId,
      });
      return;
    }

    const stop$ = entry.subscribers.get(subscriberId);
    if (stop$) {
      this.logger.debug('Unsubscribing subscriber from route', {
        route,
        subscriberId,
        remainingBeforeRemoval: entry.subscribers.size,
      });
      stop$.next();
      stop$.complete();
      // entry.subscribers.delete happens in cleanupSubscriber via finalize
    }
  }

  /** Number of active routes currently open on this connection. */
  get activeRouteCount(): number {
    return this._routes.size;
  }

  /** Number of named subscribers for a given route. */
  subscriberCountFor(route: string): number {
    return this._routes.get(route)?.subscribers.size ?? 0;
  }

  // ── Lifecycle ─────────────────────────────────────────────────

  ngOnDestroy(): void {
    this.logger.info('Service destroyed — tearing down all routes and connection', {
      activeRoutes: this._routes.size,
    });

    this._routes.forEach((entry, route) => {
      entry.subscribers.forEach((s$) => { s$.next(); s$.complete(); });
      entry.subscribers.clear();
      entry.pipeSub?.unsubscribe();
      entry.requester?.cancel();
    });
    this._routes.clear();

    this.clearActivityTimer();
    this._destroy$.next();
    this._destroy$.complete();
  }

  // ── Protected helpers (available to subclasses) ───────────────

  /**
   * Encodes an RSocket route string into composite metadata bytes,
   * using the official `rsocket-composite-metadata` package.
   * Used inside openRoute() for every requestStream call.
   */
  protected encodeRoute(route: string): Buffer {
    return encodeCompositeMetadata([
      [WellKnownMimeType.MESSAGE_RSOCKET_ROUTING, encodeRoute(route)],
    ]);
  }

  /**
   * Maximum request(n) value for "request all" semantics on a stream.
   * 0x7fffffff (2^31 - 1) is the protocol-defined max for REQUEST_N.
   */
  protected readonly MAX_REQUEST_N = 0x7fffffff;

  // ── Private helpers ───────────────────────────────────────────

  /**
   * Opens the RSocket requestStream for a route entry and pipes
   * incoming frames into entry.data$. Called:
   *   - immediately if the socket is already connected when
   *     subscribeRoute() is first called for this route
   *   - for every existing route when connect() (re)establishes
   *     the socket (covers reconnect-after-drop)
   */
  private openRoute(route: string, entry: RouteEntry<T>, socket: RSocket): void {
    if (entry.requester) {
      this.logger.debug('Route already open — skipping re-open', { route });
      return;
    }

    this.logger.debug('Opening RSocket requestStream for route', { route });

    const frame$ = new Observable<Buffer>((observer) => {
      const requester = socket.requestStream(
        {
          data:     Buffer.alloc(0),
          metadata: this.encodeRoute(route),
        },
        this.MAX_REQUEST_N,
        {
          onNext: (payload, isComplete) => {
            if (payload.data) observer.next(payload.data as Buffer);
            if (isComplete) observer.complete();
          },
          onError:     (err) => observer.error(err),
          onComplete:  ()    => observer.complete(),
          onExtension: ()    => { /* no-op */ },
        }
      );

      entry.requester = requester;

      return () => {
        try { requester.cancel(); } catch { /* already terminated */ }
        entry.requester = undefined;
      };
    });

    entry.pipeSub = frame$
      .pipe(takeUntil(this._destroy$))
      .subscribe({
        next: (buf) => {
          this.onMessageReceived();
          entry.frameCount++;

          // Periodic debug heartbeat — avoids per-frame log spam while
          // still surfacing throughput for high-frequency routes.
          if (entry.frameCount % 100 === 0) {
            this.logger.debug('Route frame heartbeat', {
              route,
              framesReceived: entry.frameCount,
            });
          }

          try {
            entry.data$.next(this.parseFrame(buf));
          } catch (e) {
            this.logger.error('parseFrame failed for route', {
              route,
              error: (e as Error).message,
              frameBytes: buf.length,
            });
          }
        },
        error: (err) => {
          // A single route erroring does not tear down the whole
          // connection — the connection-level state/retry is driven
          // by the socket's own onClose.
          this.logger.warn('Route stream error — connection unaffected', {
            route,
            error: err.message,
            framesReceived: entry.frameCount,
          });
          entry.requester = undefined;
          entry.pipeSub = undefined;
        },
        complete: () => {
          this.logger.debug('Route stream completed', {
            route,
            framesReceived: entry.frameCount,
          });
          entry.requester = undefined;
          entry.pipeSub = undefined;
        },
      });
  }

  /**
   * Removes a subscriber from a route's subscriber map. If no
   * subscribers remain, cancels the route's requestStream and
   * removes the route entry entirely.
   */
  private cleanupSubscriber(route: string, subscriberId: string): void {
    const entry = this._routes.get(route);
    if (!entry) return;

    entry.subscribers.delete(subscriberId);

    if (entry.subscribers.size === 0) {
      this.logger.info('Last subscriber left — closing route', {
        route,
        framesReceived: entry.frameCount,
      });
      entry.pipeSub?.unsubscribe();
      entry.requester?.cancel();
      entry.data$.complete();
      this._routes.delete(route);
    } else {
      this.logger.debug('Subscriber removed from route', {
        route,
        remainingSubscribers: entry.subscribers.size,
      });
    }
  }

  private onMessageReceived(): void {
    const now  = new Date();
    const isFirstConnect = !this.state().lastConnectedAt;
    const wasInactive = this.state().activity !== 'active';

    this.patch({
      status: 'connected',
      activity: 'active',
      lastMessageAt: now,
      ...(isFirstConnect && { lastConnectedAt: now }),
      error: null,
    });

    if (wasInactive) {
      this.logger.debug('Activity resumed', {
        host: this.config.host,
        port: this.config.port,
      });
    }

    this.resetActivityTimer();
  }

  private resetActivityTimer(): void {
    this.clearActivityTimer();

    this.activityTimer = setTimeout(() => {
      this.logger.debug('No messages received within idle threshold — activity: idle', {
        host: this.config.host,
        port: this.config.port,
        idleThresholdMs: this.idleThresholdMs,
      });
      this.patch({ activity: 'idle' });

      this.activityTimer = setTimeout(() => {
        this.logger.warn('No messages received within stale threshold — activity: stale', {
          host: this.config.host,
          port: this.config.port,
          staleThresholdMs: this.staleThresholdMs,
        });
        this.patch({ activity: 'stale' });
      }, this.staleThresholdMs - this.idleThresholdMs);
    }, this.idleThresholdMs);
  }

  private clearActivityTimer(): void {
    clearTimeout(this.activityTimer);
    this.activityTimer = undefined;
  }

  private patch(partial: Partial<StreamState>): void {
    this.state.update((s) => ({ ...s, ...partial }));
  }
}
