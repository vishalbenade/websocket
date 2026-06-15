// ─────────────────────────────────────────────────────────────
//  app.config.streaming.ts
//
//  Wires StreamOrchestratorService.connectAll() into app bootstrap
//  so each stream service's ONE connection is opened exactly once,
//  before any feature components render.
//
//  Merge the `providers` array below into your main app.config.ts.
// ─────────────────────────────────────────────────────────────

import { APP_INITIALIZER, ApplicationConfig, inject } from '@angular/core';
import { StreamOrchestratorService } from './core/streaming';

/**
 * Factory for APP_INITIALIZER.
 *
 * connectAll() is fire-and-forget from Angular's perspective —
 * RSocket connection + retry happens asynchronously in the
 * background via BaseStreamService's signals. We don't block
 * app startup waiting for sockets to open; components render
 * immediately and reactively pick up 'connecting' → 'connected'
 * via the status signals.
 *
 * If you DO want to block startup until connected (e.g. show a
 * splash screen until the market data feed is live), return a
 * Promise that resolves on the first 'connected' state instead —
 * see the commented alternative below.
 */
function initStreams(): () => void {
  const orchestrator = inject(StreamOrchestratorService);
  return () => {
    orchestrator.connectAll();
  };
}

export const streamingProviders: ApplicationConfig['providers'] = [
  {
    provide: APP_INITIALIZER,
    useFactory: initStreams,
    multi: true,
  },
];

// ── Alternative: block until first connection ──────────────────
//
// function initStreamsBlocking(): () => Promise<void> {
//   const orchestrator = inject(StreamOrchestratorService);
//   return () =>
//     new Promise<void>((resolve) => {
//       orchestrator.connectAll();
//       const check = setInterval(() => {
//         if (orchestrator.allLive()) {
//           clearInterval(check);
//           resolve();
//         }
//       }, 100);
//       // Always resolve after a timeout so a slow/down service
//       // doesn't block the app forever
//       setTimeout(() => { clearInterval(check); resolve(); }, 5_000);
//     });
// }
