import { SocketRouterProvider } from '@core/providers/infrastructure/SocketRouterProvider';
import { LogProvider } from '@core/providers/LogProvider'


const NAME = 'Broadcast Provider';
const BROADCAST_EVENT = 'broadcastEvent';

export class BroadcastProvider {
  broadcaster: SocketRouterProvider;
  private zLog: LogProvider;

  constructor(private address: string, private broadcastPort: string, private protocol = 'tcp', broadcaster?: SocketRouterProvider) {
    if (! broadcaster) this.broadcaster = new SocketRouterProvider(this.broadcastPort, this.protocol, NAME, BROADCAST_EVENT);
  }

  async run() {
    try {
      this.broadcastQueueOn();
      this.broadcaster.startRouter();
    } catch (err) {
      this.zLog.error(err);
      throw err;
    }
  }

  private broadcastQueueOn() {
    const formatMessage = (msg: any): string => { return JSON.stringify(msg); };
    this.broadcaster.queueListener(formatMessage, 'broadcast');
  }
}