/// <reference lib="webworker" />
// ─────────────────────────────────────────────────────────────
//  computation.worker.ts
//  Runs in a dedicated Web Worker thread.
//  Handles all CPU-bound tasks so the main thread stays free
//  for rendering and user interaction.
//
//  Tasks:
//    PARSE_ORDERBOOK  — JSON.parse of large order book snapshots
//    CALC_GREEKS      — Black-Scholes options Greeks
//    CALC_PNL         — P&L aggregation across position sets
// ─────────────────────────────────────────────────────────────

import {
  WorkerRequest,
  WorkerResponse,
  GreeksInput,
  Greeks,
  PnlInput,
  PnlResult,
  OrderBook,
} from './worker-bridge.types';

addEventListener('message', ({ data }: MessageEvent<WorkerRequest>) => {
  const { id, task, payload } = data;

  try {
    let result: unknown;

    switch (task) {
      case 'PARSE_ORDERBOOK':
        result = parseOrderBook(payload as string);
        break;

      case 'CALC_GREEKS':
        result = calcGreeks(payload as GreeksInput);
        break;

      case 'CALC_PNL':
        result = calcPnl(payload as PnlInput);
        break;

      default:
        throw new Error(`Unknown worker task: ${task}`);
    }

    postMessage({ id, result, error: null } satisfies WorkerResponse);

  } catch (err) {
    postMessage({
      id,
      result: null,
      error: (err as Error).message,
    } satisfies WorkerResponse);
  }
});

// ── Task implementations ──────────────────────────────────────

/**
 * Parses a raw JSON string into a typed OrderBook.
 * Offloaded because large snapshots (1000+ price levels) can block
 * the main thread for 10–50ms.
 */
function parseOrderBook(raw: string): OrderBook {
  return JSON.parse(raw) as OrderBook;
}

/**
 * Calculates Black-Scholes options Greeks.
 * Pure CPU — must never run on the main thread.
 */
function calcGreeks(input: GreeksInput): Greeks {
  const { S, K, T, r, sigma } = input;

  const sqrtT = Math.sqrt(T);
  const d1    = (Math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrtT);
  const d2    = d1 - sigma * sqrtT;

  const nd1  = normCdf(d1);
  const nd2  = normCdf(d2);
  const npd1 = normPdf(d1);

  return {
    delta: nd1,
    gamma: npd1 / (S * sigma * sqrtT),
    // Theta expressed as daily decay (divide annualised by 365)
    theta: (-(S * npd1 * sigma) / (2 * sqrtT) - r * K * Math.exp(-r * T) * nd2) / 365,
    vega:  S * npd1 * sqrtT * 0.01, // per 1% move in IV
  };
}

/**
 * Aggregates realised and unrealised P&L across all positions.
 * Offloaded when position count is large (hundreds to thousands).
 */
function calcPnl(input: PnlInput): PnlResult {
  return input.positions.reduce(
    (acc, pos) => ({
      realised:   acc.realised   + pos.realisedPnl,
      unrealised: acc.unrealised + (pos.currentPrice - pos.avgCost) * pos.qty,
    }),
    { realised: 0, unrealised: 0 }
  );
}

// ── Math helpers ──────────────────────────────────────────────

/** Standard normal CDF — Abramowitz & Stegun approximation (error < 7.5e-8) */
function normCdf(x: number): number {
  return 0.5 * (1 + erf(x / Math.SQRT2));
}

/** Standard normal PDF */
function normPdf(x: number): number {
  return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI);
}

/** Error function approximation */
function erf(x: number): number {
  const a1 =  0.254829592;
  const a2 = -0.284496736;
  const a3 =  1.421413741;
  const a4 = -1.453152027;
  const a5 =  1.061405429;
  const p  =  0.3275911;

  const sign = x >= 0 ? 1 : -1;
  const ax   = Math.abs(x);
  const t    = 1 / (1 + p * ax);
  const y    = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1)
                   * t * Math.exp(-ax * ax);

  return sign * y;
}
