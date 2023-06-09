import * as Redis from 'ioredis';

import { RedisProvider } from '@core/providers/dataAccess/RedisProvider';
import { QueueOpts, BPOPResp } from '@core/models/queue/Queue';


export class QueueProvider {
  private redisClient: Redis;
  private queueName: string;

  constructor(opts: QueueOpts) {
    this.redisClient = new RedisProvider(opts?.redisOpts).getClient('Queue');
    this.queueName = opts.queueName;
  }

  async leftPush(elements: any[]): Promise<boolean> {
    await this.redisClient.lpush(this.queueName, ...elements);
    return true;
  }

  async rightPush(elements: any[]): Promise<boolean> {
    await this.redisClient.rpush(this.queueName, ...elements);
    return true;
  }

  async blockingLeftPop(opts: { timeout: number, waitIndefinitely?: boolean }): Promise<BPOPResp> {
    return this.redisClient.blpop(this.queueName, opts?.waitIndefinitely ? 0 : opts.timeout);
  }

  async blockingRightPop(opts: { timeout: number, waitIndefinitely?: boolean }): Promise<BPOPResp> {
    return this.redisClient.brpop(this.queueName, opts?.waitIndefinitely ? 0 : opts.timeout);
  }
}