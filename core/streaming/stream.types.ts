// ─────────────────────────────────────────────────────────────
//  stream.types.ts
//  Central type definitions for the RSocket streaming layer.
// ─────────────────────────────────────────────────────────────

export type StreamStatus =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'disconnected'
  | 'error';

export type StreamActivity = 'active' | 'idle' | 'stale';

export interface StreamState {
  status: StreamStatus;
  activity: StreamActivity;
  retryCount: number;
  lastMessageAt: Date | null;
  lastConnectedAt: Date | null;
  error: Error | null;
}

export interface StreamConfig {
  /** WebSocket host of the target microservice */
  host: string;
  /** WebSocket port of the target microservice */
  port: number;
  /** Milliseconds of silence before activity flips to 'idle'. Default: 5 000 */
  idleThresholdMs?: number;
  /** Milliseconds of silence before activity flips to 'stale'. Default: 30 000 */
  staleThresholdMs?: number;
  /** Maximum reconnection attempts before entering 'error' state. Default: 10 */
  maxRetries?: number;
  /** Base delay for first retry in ms. Default: 1 000 */
  initialRetryDelayMs?: number;
  /** Upper cap for exponential backoff delay in ms. Default: 30 000 */
  maxRetryDelayMs?: number;
}

export const INITIAL_STREAM_STATE: StreamState = {
  status: 'idle',
  activity: 'idle',
  retryCount: 0,
  lastMessageAt: null,
  lastConnectedAt: null,
  error: null,
};
