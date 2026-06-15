# Market Stream — Architecture Reference

## ⚠ Architecture: ONE connection per service, MANY routes

Each domain stream service (`MarketDataStreamService`, `OptionsStreamService`, ...)
represents **one microservice** (one `host:port`) and opens **exactly one RSocket
connection**, established **once at app startup** via
`StreamOrchestratorService.connectAll()`.

Feature components do **not** call `connect()`/`disconnect()`. Instead they call
route-scoped methods (`ticksFor(symbol, id)`, `chainFor(underlying, id)`, or the
generic `subscribeRoute(route, id)` / `unsubscribe(route, id)`), each of which opens
or closes an independent RSocket `requestStream` **on the shared connection**.
Multiple components can subscribe to the same route, or to different routes on the
same connection, all reusing the one socket.

```
                 ┌────────────────────────────────────────┐
                 │   MarketDataStreamService (singleton)    │
                 │   ONE RSocket connection — opened once   │
                 │   at app startup via connectAll()        │
                 └───────────────┬────────────────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                          │                          │
   route: market.ticks.AAPL  route: market.ticks.MSFT   route: market.ticks.TSLA
        │                          │                          │
   ┌────┴────┐              ┌──────┴──────┐            ┌──────┴──────┐
   │ GridA   │              │ GridA       │            │ GridB       │
   │ (AAPL)  │              │ (MSFT)      │            │ (TSLA)      │
   └─────────┘              └─────────────┘            └─────────────┘
```

---

## File Structure

```
src/app/
├── app.config.streaming.ts                     ← APP_INITIALIZER: connectAll() at startup
└── core/
    └── streaming/
        ├── index.ts                            ← public barrel export
        ├── stream.types.ts                     ← StreamState, StreamConfig, StreamStatus
        ├── retry-strategy.service.ts           ← exponential backoff + jitter (+ logging)
        ├── rsocket-client.factory.ts           ← cold Observable<RSocket> (socket only, + logging)
        ├── base-stream.service.ts              ← ONE connection + MANY routes (signals, retry, subscribeRoute/unsubscribe, + logging)
        ├── stream-orchestrator.service.ts      ← connectAll() at startup + global health
        ├── logging/
        │   └── stream-logger.ts                ← createStreamLogger() — wraps @your-org/logger LoggerService
        ├── worker/
        │   ├── worker-bridge.types.ts          ← WorkerRequest/Response + task payload types
        │   ├── worker-bridge.service.ts        ← main-thread interface, UUID correlation, + logging
        │   └── computation.worker.ts           ← Black-Scholes, P&L, JSON parse (off main thread)
        └── streams/
            ├── market-data-stream.service.ts   ← route: market.ticks.<symbol>
            ├── options-stream.service.ts       ← route: options.chain.<underlying>, Greeks via worker
            ├── order-stream.service.ts
            ├── position-stream.service.ts
            └── risk-stream.service.ts

src/app/features/
└── market-grid/
    ├── market-grid.component.ts                ← OnPush + signals status bar, @Input symbols
    ├── market-grid-adapter.service.ts           ← opens/closes per-symbol routes on shared connection
    └── market-grid.columns.ts                  ← ColDef[] + formatters
```

---

## Connection state machine (per service, established ONCE)

```
idle
 └─ connect() ──────────────────────► connecting     (called once, at app startup
                                           │            via StreamOrchestratorService.connectAll())
                                    socket opens
                                           │
                                    first message ──► connected
                                    (on ANY route)     │
                                           │              │
                                    socket error    message arrives on any route
                                           │         → activity: active
                                    retry delay            │
                                           │         silence on ALL routes > idleThresholdMs
                                      reconnecting          │
                                           │         activity: idle
                                    max retries             │
                                    exceeded         silence > staleThresholdMs
                                           │              │
                                        error        activity: stale
                                           │
                                    disconnect() ──► disconnected
                                    (cancels all routes,
                                     keeps subscriber
                                     registrations for
                                     re-open on reconnect)
```

`status`/`activity` reflect the **connection**, aggregated across all routes
currently open on it — not any single route.

---

## Activity states

| `activity` | Meaning | Threshold |
|---|---|---|
| `active` | Messages arriving within threshold | Within `idleThresholdMs` |
| `idle` | Connected but silent | `idleThresholdMs` (default 5s) |
| `stale` | Prolonged silence — feed likely dead | `staleThresholdMs` (default 30s) |

---

## AG Grid real-time update pipeline

```
RSocket frame (route: market.ticks.AAPL, market.ticks.MSFT, ...)
    │
    ▼
per-route data$ Subject            ← hot, multicast to all subscribers of that route
    │
    ▼ ticksFor('AAPL', adapterId), ticksFor('MSFT', adapterId), ...
merge(...)                         ← fan multiple symbol routes into one stream
    │
    ▼
bufferTime(100ms)                  ← Stage 1: collapse N ticks → 1 array
    │
    ▼
deduplication (last tick/symbol)   ← prevent redundant row flashes
    │
    ▼
applyTransactionAsync({ update })  ← Stage 2: AG Grid internal queue
    │
    ▼
requestAnimationFrame flush        ← AG Grid paints on next frame
```

### Buffer window tuning guide

| Tick frequency | `bufferTime` | `asyncTransactionWaitMillis` |
|---|---|---|
| < 10 / sec     | 50 ms        | 50 ms  |
| 10–100 / sec   | 100 ms       | 100 ms |
| 100–500 / sec  | 150 ms       | 100 ms |
| 500+ / sec     | 200 ms       | 50 ms  |

---

## Route subscription lifecycle (per symbol/route, reusing one connection)

```typescript
// App startup (once) — opens the ONE socket for this service
streamOrchestrator.connectAll();

// Component A opens a route — first subscriber opens the RSocket requestStream
const aaplA$ = marketStream.ticksFor('AAPL', 'component-a');

// Component B opens the SAME route — reuses the existing requestStream,
// no new RSocket subscription is created
const aaplB$ = marketStream.ticksFor('AAPL', 'component-b');

// Component A leaves — route stays open because component-b still uses it
marketStream.unsubscribeTicks('AAPL', 'component-a');

// Component B leaves — last subscriber for 'AAPL', so the underlying
// RSocket requestStream IS cancelled. The SOCKET CONNECTION stays open.
marketStream.unsubscribeTicks('AAPL', 'component-b');

// The connection itself is only closed via disconnect() — typically
// only on logout via streamOrchestrator.disconnectAll()
```

### Generic API (for custom routes / other services)

```typescript
const obs$ = someStreamService.subscribeRoute('custom.route.NAME', 'my-id');
someStreamService.unsubscribe('custom.route.NAME', 'my-id');

someStreamService.activeRouteCount;            // how many routes open on this connection
someStreamService.subscriberCountFor('route'); // how many subscribers for one route
```

---

## Signal surface (all readonly, use in templates without async pipe)

| Signal | Type | Description |
|---|---|---|
| `status` | `StreamStatus` | Connection phase |
| `activity` | `StreamActivity` | Data flow health |
| `isLive` | `boolean` | `status === 'connected'` |
| `isStreaming` | `boolean` | `activity === 'active'` |
| `isStale` | `boolean` | `activity === 'stale'` |
| `lastMessageAt` | `Date \| null` | Timestamp of last received message |
| `lastConnectedAt` | `Date \| null` | First connection timestamp |
| `error` | `Error \| null` | Last terminal error |
| `retryCount` | `number` | Current retry attempt |

---

## App bootstrap — connect once at startup

Merge `streamingProviders` from `app.config.streaming.ts` into your main
`app.config.ts`:

```typescript
// app.config.ts
import { ApplicationConfig } from '@angular/core';
import { streamingProviders } from './app.config.streaming';

export const appConfig: ApplicationConfig = {
  providers: [
    // ...your existing providers
    ...streamingProviders,
  ],
};
```

This runs `StreamOrchestratorService.connectAll()` via `APP_INITIALIZER` —
opening one socket per registered service before any route is subscribed.
Components can render immediately; `status()`/`activity()` signals reactively
show `'connecting'` → `'connected'`.

---

## Adding a new route-based stream service

1. Create `streams/<name>-stream.service.ts` extending `BaseStreamService<TRaw>`
2. Set `config: StreamConfig` — **no `route` field** (host/port + thresholds only)
3. Implement `parseFrame(buffer): TRaw` — sync decode of any frame on this connection
4. Expose typed wrapper methods, e.g.:
   ```typescript
   positionsFor(account: string, subscriberId: string) {
     return this.subscribeRoute(`positions.${account}`, subscriberId);
   }
   unsubscribePositions(account: string, subscriberId: string) {
     this.unsubscribe(`positions.${account}`, subscriberId);
   }
   ```
5. Register it in `StreamOrchestratorService.streams` so `connectAll()` opens its socket

---

## Key Angular 19 patterns used

- **`signal()` + `computed()`** — state and derived views, no BehaviorSubject
- **`ChangeDetectionStrategy.OnPush`** — grid ticks never trigger Angular CD
- **`providers: [MarketGridAdapterService]`** — component-scoped, auto-destroyed
- **`toSignal()`** — RxJS → signal bridge in simple components
- **`@if` / `@for`** — control flow syntax (Angular 17+)

---

## Web Worker — when and why

Web Workers are used only for CPU-bound tasks. WebSocket I/O is non-blocking and stays on the main thread.

| Task | Main thread | Worker |
|---|---|---|
| Receiving RSocket frames | ✓ | |
| `JSON.parse` small tick payloads | ✓ | |
| `JSON.parse` large order book snapshots | | ✓ |
| Black-Scholes Greeks calculation | | ✓ |
| P&L aggregation across large position sets | | ✓ |
| IV surface construction | | ✓ |

### Worker integration pattern (per-route wrapper method)

```typescript
chainFor(underlying: string, subscriberId: string): Observable<OptionChainRow> {
  return this.subscribeRoute(`options.chain.${underlying}`, subscriberId).pipe(
    mergeMap((frame) =>
      from(this.workerBridge.run<GreeksInput, Greeks>('CALC_GREEKS', input))
        .then((greeks) => ({ ...frame, greeks }))
    )
  );
}
```

`parseFrame()` stays a cheap synchronous decode shared by all routes on the
connection; the worker hop is added only in the typed wrapper method that
needs it. It is invisible to `BaseStreamService`, the grid adapter, and components.

---

## Install dependencies

```bash
npm install rsocket-core@1.0.0-alpha.3 rsocket-websocket-client@1.0.0-alpha.3 rsocket-composite-metadata@1.0.0-alpha.3
npm install ag-grid-community ag-grid-angular
```

> **Note on rsocket-core@1.0.0-alpha.3 API**
> This version replaced the 0.x `RSocketClient` API with `RSocketConnector`
> (returns `Promise<RSocket>` from `.connect()`). `MAX_STREAM_ID` is no
> longer exported — use the protocol max `0x7fffffff` (exposed here as
> `BaseStreamService.MAX_REQUEST_N`). `requestStream()` no longer has an
> `onSubscribe` callback; it returns a `Cancellable & Requestable` handle
> directly, which `openRoute()` calls `.cancel()` on during RxJS teardown.
> `payload.data` must be a `Buffer` (use `Buffer.alloc(0)` for empty bodies)
> — requires `@types/node` for `Buffer` typings under TypeScript 5.7.2.
>
> **Composite metadata / routing**
> `encodeRoute()` in `base-stream.service.ts` uses
> `encodeCompositeMetadata`, `encodeRoute`, and `WellKnownMimeType` from
> `rsocket-composite-metadata@1.0.0-alpha.3` directly. If your build
> reports "Cannot find module 'rsocket-composite-metadata'", confirm it's
> installed (`npm ls rsocket-composite-metadata`) and that `tsconfig.json`
> uses `"moduleResolution": "bundler"` or `"node16"`/`"nodenext"`.

---

## Logging

Every file in `core/streaming/` injects a context-scoped logger via
`createStreamLogger(contextName)` (see `logging/stream-logger.ts`), which
wraps your existing `@your-org/logger` `LoggerService.forContext(...)`.
Each log line is tagged with its source (`MarketDataStreamService`,
`RSocketClientFactory`, `RetryStrategyService`, `WorkerBridgeService`,
`MarketGridAdapterService`, etc.) and carries structured `meta` —
no raw payload bodies are logged (PII-safe).

| Level | When | Examples |
|---|---|---|
| `debug` | High-frequency / per-frame events | route opened, frame heartbeat (every 100 frames), retry delay computed, worker task dispatched/completed |
| `info` | Lifecycle milestones | connecting/connected/reconnected/disconnected, route opened/closed, adapter attach/detach, symbol watchlist changes |
| `warn` | Recoverable issues | retry attempt, route-level error (connection unaffected), activity → `stale`, socket closed with error |
| `error` | Terminal failures | max retries exceeded, `parseFrame` failure, worker task failure |

If your `LoggerService` API differs from
`forContext(context).{debug,info,warn,error}(message, meta?)`, adjust
only `logging/stream-logger.ts` — every other file is unaffected.
