// ─────────────────────────────────────────────────────────────
//  market-grid.columns.ts
//  AG Grid column definitions for the MarketTick row model.
//  Separated from the component to keep it testable and reusable.
// ─────────────────────────────────────────────────────────────

import {
  ColDef,
  ValueFormatterParams,
  CellClassParams,
  ICellRendererParams,
} from 'ag-grid-community';
import { MarketTick } from '../../core/streaming/streams/market-data-stream.service';

// ── Formatters ────────────────────────────────────────────────

function priceFormatter(p: ValueFormatterParams): string {
  return p.value != null ? Number(p.value).toFixed(4) : '—';
}

function sizeFormatter(p: ValueFormatterParams): string {
  if (p.value == null) return '—';
  return Number(p.value) >= 1_000
    ? `${(p.value / 1_000).toFixed(1)}K`
    : String(p.value);
}

function timestampFormatter(p: ValueFormatterParams): string {
  if (!p.value) return '—';
  return new Date(p.value).toLocaleTimeString('en-GB', {
    hour12: false,
    hour:   '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

// ── Cell class rules ──────────────────────────────────────────
// AG Grid re-evaluates these on every cell update, so keep them fast

type PriceField = 'bid' | 'ask';

function priceUpRule(field: PriceField) {
  return (p: CellClassParams<MarketTick>): boolean => {
    const prev = (p.data as any)?.[`prev_${field}`];
    return prev != null && p.value > prev;
  };
}

function priceDownRule(field: PriceField) {
  return (p: CellClassParams<MarketTick>): boolean => {
    const prev = (p.data as any)?.[`prev_${field}`];
    return prev != null && p.value < prev;
  };
}

// ── Column definitions ────────────────────────────────────────

export const MARKET_COL_DEFS: ColDef<MarketTick>[] = [
  {
    field:       'symbol',
    headerName:  'Symbol',
    width:       110,
    pinned:      'left',
    cellClass:   'market-cell--symbol',
    sort:        'asc',
  },
  {
    field:            'bid',
    headerName:       'Bid',
    width:            130,
    valueFormatter:   priceFormatter,
    enableCellChangeFlash: true,
    cellClassRules: {
      'market-cell--up':   priceUpRule('bid'),
      'market-cell--down': priceDownRule('bid'),
    },
  },
  {
    field:            'ask',
    headerName:       'Ask',
    width:            130,
    valueFormatter:   priceFormatter,
    enableCellChangeFlash: true,
    cellClassRules: {
      'market-cell--up':   priceUpRule('ask'),
      'market-cell--down': priceDownRule('ask'),
    },
  },
  {
    headerName:   'Spread',
    colId:        'spread',
    width:        110,
    // Derived — not stored in row data; recomputed on every render
    valueGetter:  (p) =>
      p.data ? +(p.data.ask - p.data.bid).toFixed(4) : null,
    valueFormatter: priceFormatter,
    cellClass: 'market-cell--spread',
  },
  {
    field:          'bidSize',
    headerName:     'Bid Size',
    width:          110,
    valueFormatter: sizeFormatter,
    cellClass:      'market-cell--size',
  },
  {
    field:          'askSize',
    headerName:     'Ask Size',
    width:          110,
    valueFormatter: sizeFormatter,
    cellClass:      'market-cell--size',
  },
  {
    field:          'timestamp',
    headerName:     'Updated',
    width:          110,
    valueFormatter: timestampFormatter,
    cellClass:      'market-cell--time',
  },
];

export const DEFAULT_COL_DEF: ColDef = {
  resizable:   true,
  sortable:    true,
  filter:      true,
  minWidth:    80,
  // Suppress the 3-dot menu on every column header
  suppressMenu: false,
};
