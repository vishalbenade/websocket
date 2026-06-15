// ─────────────────────────────────────────────────────────────
//  market-grid.component.ts
//  Real-time market data grid powered by:
//    • MarketDataStreamService  (RSocket stream)
//    • MarketGridAdapterService (RxJS → AG Grid bridge)
//    • AG Grid applyTransactionAsync (high-frequency updates)
//    • Angular signals             (status bar, zero CD overhead)
//    • ChangeDetectionStrategy.OnPush (tick data never triggers CD)
//
//  Lifecycle contract:
//    ngOnInit     → nothing (grid not ready yet)
//    gridReady    → adapter.attach(api)  — starts stream + transactions
//    ngOnDestroy  → adapter cleans up automatically (component-scoped)
// ─────────────────────────────────────────────────────────────

import {
  Component,
  OnDestroy,
  OnChanges,
  SimpleChanges,
  Input,
  inject,
  computed,
  ChangeDetectionStrategy,
} from '@angular/core';
import { DatePipe }       from '@angular/common';
import { AgGridAngular }  from 'ag-grid-angular';
import {
  GridReadyEvent,
  GridOptions,
  GetRowIdParams,
} from 'ag-grid-community';

import { MarketDataStreamService, MarketTick } from
  '../../core/streaming/streams/market-data-stream.service';
import { MarketGridAdapterService } from './market-grid-adapter.service';
import { MARKET_COL_DEFS, DEFAULT_COL_DEF } from './market-grid.columns';

@Component({
  standalone: true,
  selector:   'app-market-grid',

  // ── OnPush: tick data NEVER enters Angular change detection.
  //    Only signals (activity, status, lastMessageAt) drive re-renders
  //    in the status bar, and only when their value actually changes.
  changeDetection: ChangeDetectionStrategy.OnPush,

  // ── Component-scoped provider: adapter lifecycle is tied to this
  //    component. When destroyed, the adapter auto-detaches the stream.
  providers: [MarketGridAdapterService],

  imports: [AgGridAngular, DatePipe],

  template: `
    <!-- ── Status bar ─────────────────────────────────────── -->
    <div class="market-status-bar" [attr.data-status]="stream.status()">

      <div class="market-status-bar__left">
        <!-- Activity pulse indicator -->
        <span
          class="market-status-bar__dot"
          [attr.data-activity]="stream.activity()"
          [title]="stream.activity()"
        ></span>

        <!-- Human-readable label -->
        <span class="market-status-bar__label">{{ statusLabel() }}</span>

        <!-- Stale warning — only shown when feed goes silent -->
        @if (stream.isStale()) {
          <span class="market-status-bar__stale-badge">
            ⚠ Feed stale
          </span>
        }

        <!-- Error detail -->
        @if (stream.error()) {
          <span class="market-status-bar__error" [title]="stream.error()!.message">
            Error: {{ stream.error()!.message | slice:0:60 }}
          </span>
        }
      </div>

      <div class="market-status-bar__right">
        @if (stream.lastMessageAt()) {
          <span class="market-status-bar__meta">
            Last tick {{ stream.lastMessageAt() | date:'HH:mm:ss.SSS' }}
          </span>
        }
        @if (stream.retryCount() > 0) {
          <span class="market-status-bar__meta market-status-bar__meta--warn">
            Retry #{{ stream.retryCount() }}
          </span>
        }
      </div>
    </div>

    <!-- ── AG Grid ────────────────────────────────────────── -->
    <ag-grid-angular
      class="ag-theme-quartz-dark market-grid__table"
      [columnDefs]="colDefs"
      [defaultColDef]="defaultColDef"
      [gridOptions]="gridOptions"
      [rowData]="[]"
      (gridReady)="onGridReady($event)"
    />
  `,

  styles: [`
    :host {
      display: flex;
      flex-direction: column;
      height: 100%;
      font-family: 'IBM Plex Mono', 'Fira Code', monospace;
    }

    /* ── Status bar ─────────────────────────────────────── */
    .market-status-bar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 6px 14px;
      background: #0d0f14;
      border-bottom: 1px solid #1e2130;
      font-size: 11px;
      color: #6b7280;
      min-height: 32px;
      gap: 12px;
    }

    .market-status-bar__left,
    .market-status-bar__right {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    /* Pulse dot */
    .market-status-bar__dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      flex-shrink: 0;
      background: #374151;
      transition: background 0.3s;
    }
    .market-status-bar__dot[data-activity='active'] {
      background: #10b981;
      box-shadow: 0 0 8px #10b98166;
      animation: market-pulse 1.2s ease-in-out infinite;
    }
    .market-status-bar__dot[data-activity='idle'] {
      background: #f59e0b;
    }
    .market-status-bar__dot[data-activity='stale'] {
      background: #ef4444;
      animation: market-pulse 0.8s ease-in-out infinite;
    }

    @keyframes market-pulse {
      0%, 100% { opacity: 1; }
      50%       { opacity: 0.4; }
    }

    .market-status-bar__label {
      color: #9ca3af;
      font-weight: 500;
    }
    [data-status='connected'] .market-status-bar__label { color: #d1d5db; }
    [data-status='error']     .market-status-bar__label { color: #ef4444; }

    .market-status-bar__stale-badge {
      background: #7f1d1d;
      color: #fca5a5;
      padding: 1px 7px;
      border-radius: 3px;
      font-size: 10px;
      font-weight: 600;
      letter-spacing: 0.04em;
    }

    .market-status-bar__error {
      color: #f87171;
      font-size: 10px;
    }

    .market-status-bar__meta {
      color: #4b5563;
      font-size: 10px;
    }
    .market-status-bar__meta--warn {
      color: #f59e0b;
    }

    /* ── Grid ───────────────────────────────────────────── */
    .market-grid__table {
      flex: 1;
      width: 100%;
    }

    /* Cell flash colours — scoped under ag-theme-quartz-dark */
    :host ::ng-deep .ag-theme-quartz-dark {
      --ag-background-color: #0d0f14;
      --ag-odd-row-background-color: #0f1117;
      --ag-header-background-color: #13161f;
      --ag-border-color: #1e2130;
      --ag-row-hover-color: #161a24;
    }

    :host ::ng-deep .market-cell--symbol {
      color: #e2e8f0;
      font-weight: 600;
      letter-spacing: 0.05em;
    }

    :host ::ng-deep .market-cell--spread,
    :host ::ng-deep .market-cell--size,
    :host ::ng-deep .market-cell--time {
      color: #6b7280;
    }

    /* Price up/down flash classes */
    :host ::ng-deep .market-cell--up   { color: #10b981; }
    :host ::ng-deep .market-cell--down { color: #ef4444; }

    /* AG Grid's built-in flash override */
    :host ::ng-deep .ag-cell-data-changed-animation-up {
      background-color: #10b98133 !important;
    }
    :host ::ng-deep .ag-cell-data-changed-animation-down {
      background-color: #ef444433 !important;
    }
  `],
})
export class MarketGridComponent implements OnDestroy, OnChanges {

  readonly stream  = inject(MarketDataStreamService);
  private readonly adapter = inject(MarketGridAdapterService);

  readonly colDefs       = MARKET_COL_DEFS;
  readonly defaultColDef = DEFAULT_COL_DEF;

  readonly gridOptions: GridOptions<MarketTick> = {
    // ── Row identity — REQUIRED for applyTransactionAsync ──────
    // Without getRowId, AG Grid cannot match incoming ticks to
    // existing rows and will fall back to full re-renders.
    getRowId: (p: GetRowIdParams<MarketTick>) => p.data.symbol,

    // ── High-frequency update settings ─────────────────────────
    // AG Grid's internal async flush cadence (ms).
    // Match this to your bufferTime in the adapter.
    asyncTransactionWaitMillis: 100,

    // Enable built-in cell change flash animation
    enableCellChangeFlash: true,
    cellFlashDuration:  400,   // ms the flash colour stays
    cellFadeDuration:   300,   // ms the flash colour fades out

    // ── Performance ─────────────────────────────────────────────
    // Keep animations on — AG Grid schedules them via requestAnimationFrame
    suppressAnimationFrame: false,
    // Row virtualisation is on by default — do not disable
    suppressRowVirtualisation: false,

    // ── UI ──────────────────────────────────────────────────────
    rowModelType:      'clientSide',
    rowSelection:      'single',
    suppressCellFocus: false,
    animateRows:       true,

    // ── Status bar at the bottom of the grid ───────────────────
    statusBar: {
      statusPanels: [
        { statusPanel: 'agTotalRowCountComponent', align: 'left' },
        { statusPanel: 'agFilteredRowCountComponent' },
      ],
    },
  };

  // ── Computed status label (signal) ────────────────────────────
  readonly statusLabel = computed(() => {
    const status   = this.stream.status();
    const activity = this.stream.activity();
    const retries  = this.stream.retryCount();

    if (status === 'connected' && activity === 'active')  return '● Live';
    if (status === 'connected' && activity === 'idle')    return '◌ Connected · Waiting';
    if (status === 'connected' && activity === 'stale')   return '⚠ Connected · Stale';
    if (status === 'reconnecting') return `↻ Reconnecting (attempt ${retries})`;
    if (status === 'connecting')   return '… Connecting';
    if (status === 'error')        return '✕ Connection failed';
    if (status === 'disconnected') return '○ Disconnected';
    return '○ Idle';
  });

  // ── Grid event handlers ───────────────────────────────────────

  /**
   * Called by AG Grid once the row model and API are fully initialised.
   * This is the correct place to attach the adapter — NOT ngOnInit.
   *
   * Calling applyTransactionAsync before gridReady results in
   * silent no-ops because the row model doesn't exist yet.
   */
  /**
   * Symbols this grid instance should stream. Set via @Input — e.g.
   * a watchlist component passes its current ticker list.
   *
   * Each symbol opens its own RSocket route ('market.ticks.<symbol>')
   * on the SHARED MarketDataStreamService connection (opened once at
   * app startup). Multiple grids/components can watch the same or
   * different symbols independently over that one connection.
   */
  @Input({ required: true }) symbols: string[] = [];

  /**
   * Called by AG Grid once the row model and API are fully initialised.
   * This is the correct place to attach the adapter — NOT ngOnInit.
   *
   * Calling applyTransactionAsync before gridReady results in
   * silent no-ops because the row model doesn't exist yet.
   */
  onGridReady(event: GridReadyEvent<MarketTick>): void {
    this.adapter.attach(event.api, this.symbols);
  }

  /**
   * Call when the symbol list changes at runtime (e.g. user edits a
   * watchlist). Diffs against currently-open routes — only newly
   * added symbols open new routes; removed symbols are unsubscribed
   * (other components watching the same symbol are unaffected).
   */
  ngOnChanges(changes: SimpleChanges): void {
    if (changes['symbols'] && !changes['symbols'].firstChange) {
      this.adapter.updateSymbols(this.symbols);
    }
  }

  ngOnDestroy(): void {
    // MarketGridAdapterService is component-scoped via providers[].
    // Angular calls adapter.ngOnDestroy() automatically — which in turn
    // calls detach() → unsubscribes from the stream.
    // Nothing extra needed here.
  }
}
