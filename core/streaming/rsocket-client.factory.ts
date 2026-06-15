// ─────────────────────────────────────────────────────────────
//  rsocket-client.factory.ts
//  rsocket-core@1.0.0-alpha.3, rsocket-websocket-client@1.0.0-alpha.3
//
//  API notes (0.x → alpha.3):
//    • RSocketClient        → RSocketConnector
//    • client.connect()     → connector.connect() : Promise<RSocket>
//    • new RSocketWebSocketClient({url, wsCreator}) →
//        new WebsocketClientTransport({ url, wsCreator })
//
//  Creates cold Observable<RSocket> connections.
//  Subscribing opens the socket; unsubscribing closes it.
//  Each stream service owns its own connection.
// ─────────────────────────────────────────────────────────────

import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { RSocketConnector, RSocket } from 'rsocket-core';
import { WebsocketClientTransport } from 'rsocket-websocket-client';
import { createStreamLogger, ContextLogger } from './logging/stream-logger';

@Injectable({ providedIn: 'root' })
export class RSocketClientFactory {

  private readonly logger: ContextLogger = createStreamLogger('RSocketClientFactory');

  /**
   * Returns a cold Observable that emits the connected RSocket instance.
   *
   * - Subscribing   → opens the WebSocket + RSocket handshake
   * - Unsubscribing → closes the RSocket (teardown is automatic via RxJS)
   *
   * Do NOT share() this observable — each stream service must own
   * its connection so disconnects are isolated.
   */
  create(host: string, port: number): Observable<RSocket> {
    const url = `ws://${host}:${port}`;

    return new Observable<RSocket>((observer) => {
      let cancelled = false;
      let rsocket: RSocket | undefined;

      this.logger.debug('Opening WebSocket transport', { url });

      const connector = new RSocketConnector({
        transport: new WebsocketClientTransport({
          url,
          wsCreator: (wsUrl: string) => new WebSocket(wsUrl),
        }),
        setup: {
          // Server must respond within this window or the connection drops
          keepAlive: 30_000,
          // Connection is considered dead if no keepalive for this long
          lifetime: 180_000,
          dataMimeType: 'application/json',
          metadataMimeType: 'message/x.rsocket.composite-metadata.v0',
        },
      });

      connector
        .connect()
        .then((socket) => {
          if (cancelled) {
            // Subscriber unsubscribed before connect resolved
            this.logger.debug('Connect resolved after cancellation — closing socket', { url });
            socket.close();
            return;
          }
          rsocket = socket;
          this.logger.info('RSocket handshake complete', { url });

          // Surface unexpected socket-level closures as errors so the
          // retry strategy in BaseStreamService can react.
          socket.onClose((err?: Error) => {
            if (err) {
              this.logger.warn('Socket closed with error', { url, error: err.message });
              observer.error(err);
            } else {
              this.logger.info('Socket closed cleanly', { url });
              observer.complete();
            }
          });

          observer.next(socket);
        })
        .catch((err: Error) => {
          this.logger.error('RSocket connect() failed', { url, error: err.message });
          observer.error(err);
        });

      // Teardown: called when the outer subscription is unsubscribed
      return () => {
        cancelled = true;
        try {
          rsocket?.close();
        } catch {
          /* already closed */
        }
      };
    });
  }
}
