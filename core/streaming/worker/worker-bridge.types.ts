// ─────────────────────────────────────────────────────────────
//  worker-bridge.types.ts
//  Shared types between the main thread (WorkerBridgeService)
//  and the computation worker (computation.worker.ts).
// ─────────────────────────────────────────────────────────────

export type WorkerTask = 'PARSE_ORDERBOOK' | 'CALC_GREEKS' | 'CALC_PNL';

export interface WorkerRequest {
  /** UUID correlating each request to its response */
  id: string;
  task: WorkerTask;
  payload: unknown;
}

export interface WorkerResponse {
  id: string;
  result: unknown;
  error: string | null;
}

// ── Task payload types ────────────────────────────────────────

export interface GreeksInput {
  S: number;      // Current underlying price
  K: number;      // Strike price
  T: number;      // Time to expiry in years
  r: number;      // Risk-free rate
  sigma: number;  // Implied volatility
}

export interface Greeks {
  delta: number;
  gamma: number;
  theta: number;
  vega:  number;
}

export interface PnlInput {
  positions: Array<{
    symbol:       string;
    qty:          number;
    avgCost:      number;
    currentPrice: number;
    realisedPnl:  number;
  }>;
}

export interface PnlResult {
  realised:   number;
  unrealised: number;
}

export interface OrderBook {
  symbol: string;
  bids: Array<{ price: number; size: number }>;
  asks: Array<{ price: number; size: number }>;
  timestamp: number;
}
