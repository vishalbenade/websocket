// ─────────────────────────────────────────────────────────────
//  core/streaming/index.ts
//  Public API barrel — import from here, not from individual files.
//
//  Usage:
//    import { MarketDataStreamService } from '@core/streaming';
// ─────────────────────────────────────────────────────────────

// Types
export * from './stream.types';

// Infrastructure (rarely imported directly by features)
export { RetryStrategyService }  from './retry-strategy.service';
export { RSocketClientFactory }  from './rsocket-client.factory';
export { BaseStreamService }     from './base-stream.service';

// Logging
export { createStreamLogger }       from './logging/stream-logger';
export type { ContextLogger }       from './logging/stream-logger';

// Orchestrator
export { StreamOrchestratorService } from './stream-orchestrator.service';

// Worker layer
export { WorkerBridgeService }                 from './worker/worker-bridge.service';
export type { WorkerTask, GreeksInput, Greeks,
              PnlInput, PnlResult, OrderBook } from './worker/worker-bridge.types';

// Domain stream services
export { MarketDataStreamService }             from './streams/market-data-stream.service';
export type { MarketTick }                     from './streams/market-data-stream.service';
export { OptionsStreamService }                from './streams/options-stream.service';
export type { OptionChainRow, RawOptionFrame } from './streams/options-stream.service';
// export { OrderStreamService }               from './streams/order-stream.service';
// export { PositionStreamService }            from './streams/position-stream.service';
// export { RiskStreamService }                from './streams/risk-stream.service';
