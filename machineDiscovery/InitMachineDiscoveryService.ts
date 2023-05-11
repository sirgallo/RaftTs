import { BaseServer } from '@baseServer/core/BaseServer';
import { LogProvider } from '@core/providers/LogProvider';
import { SocketRouterProvider } from '@core/providers/infrastructure/SocketRouterProvider';
import { MachineMapProvider } from '@core/providers/infrastructure/MachineDiscoveryProvider';


export class InitMachineDiscoveryService extends BaseServer {
  private zLog: LogProvider = new LogProvider(`${this.name} Init`);
  constructor(
    private basePath: string, private socketOpts: { port: string, protocol: string, queueEventName: string },
    private serverOpts: { name: string, port?: number, version?: string, numOfCpus?: number }
  ) { 
    super(serverOpts.name, serverOpts.port, serverOpts.version, serverOpts.numOfCpus); 
  }

  async startServer() {
    try {
      const sock = new SocketRouterProvider(
        this.socketOpts.port, this.socketOpts.protocol, 
        this.name, this.socketOpts.queueEventName
      );
      const machineDiscovery = new MachineMapProvider(sock);

      machineDiscovery.run();
      this.run();
    } catch (err) {
      this.zLog.error(err);
      throw err;
    }
  }
}