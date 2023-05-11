import { KnownMachinesMap, HeartBeat } from "@core/models/infrastructure/Mq";
import { LogProvider } from "../LogProvider";
import { SocketRouterProvider } from "./SocketRouterProvider";


const strEncoding = 'utf-8';

export class MachineDiscoveryProvider {
  private zLog = new LogProvider('Machine Discovery Provider');
  constructor(private sock: SocketRouterProvider, private opts: { endpoint: string, port?: number }, private protocol = 'tcp') {}

  async getKnownMachines(): Promise<KnownMachinesMap> {
    try { 
      const heartbeat: HeartBeat = {
        routerId: this.sock.sock.routingId,
        healthy: 'Alive',
        status: 'Ready'
      };
  
      let endpoint = `${this.protocol}://${this.opts.endpoint}`;
      if (this.opts?.port) endpoint = endpoint + `:${this.opts.port}`;
      
      await this.sock.sock.bind(endpoint);
      this.zLog.info('socket bound to machine discovery service');

      await this.sock.sock.send(JSON.stringify(heartbeat));
      this.zLog.info('heartbeat sent');

      for await (const [ _, body ] of this.sock.sock) {
        const machineMap: KnownMachinesMap = JSON.parse(body.toString(strEncoding));
        this.zLog.info('returning machine mapping to host');
        
        return machineMap;
      }
    } catch (err) { this.zLog.error(err); }
  }
}

export class MachineMapProvider {
  private zLog = new LogProvider('Machine Map Provider');
  constructor(private sock: SocketRouterProvider) {}

  async run() {
    try {
      const sockResponseHandler = async (strHeader: string, jsonBody: HeartBeat) => {
        await this.sock.sock.send([ strHeader, JSON.stringify(this.sock.knownMachinesMap) ])
      };

      this.sock.startRouter(sockResponseHandler.bind(this))
    } catch (err) { this.zLog.error(err); }
  }
}