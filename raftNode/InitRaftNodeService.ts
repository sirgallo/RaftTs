import { BaseServer } from '@baseServer/core/BaseServer';
import { LogProvider } from '@core/providers/LogProvider';
import { NodeAttributes } from '@raftNode/models/Node';


export class InitRaftNodeService extends BaseServer implements NodeAttributes {
  currentTerm = 0;
  votedFor: string;

  private ledgerInitLog: LogProvider = new LogProvider(`${this.name} Init`);

  constructor(private basePath: string, name: string, port?: number, version?: string, numOfCpus?: number) { 
    super(name, port, version, numOfCpus); 
  }

  async startServer() {
    try {

      this.run();
    } catch (err) {
      this.ledgerInitLog.error(err);
      throw err;
    }
  }
}