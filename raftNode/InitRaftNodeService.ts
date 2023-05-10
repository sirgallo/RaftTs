import { randomBytes } from 'crypto';

import { BaseServer } from '@baseServer/core/BaseServer';
import { mergeBaseRoutePath } from '@baseServer/core/utils/mergeBaseRoutePath';
import { LogProvider } from '@core/providers/LogProvider';
import { SimpleQueueProvider } from '@core/providers/queue/SimpleQueueProvider';

export class InitRaftNodeService extends BaseServer {
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