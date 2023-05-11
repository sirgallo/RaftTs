import { RedisOptions, Redis } from 'ioredis';

import { RedisProvider } from '@core/providers/dataAccess/RedisProvider';
import { RedisServiceType } from '@core/models/dataAccess/Redis';


interface CacheOpts {
  expirationInSec?: number;
  serviceType: RedisServiceType;
  prefix: string;
}

export class KeyValProvider {
  redisClient: Redis;
  
  private prefix: string;
  private expirationInSec: number;

  constructor(opts: { cacheOpts: CacheOpts, redisOpts?: RedisOptions }) {
    this.prefix = opts.cacheOpts.prefix;
    this.expirationInSec = opts.cacheOpts.expirationInSec;
    this.redisClient = new RedisProvider(opts.redisOpts).getClient(opts.cacheOpts.serviceType);
  }

  prefixedKey = (key: string): string => `${this.prefix}.${key}`;

  async set(key: string, value: string): Promise<boolean> {
    const prefixedKey = this.prefixedKey(key);
    await this.redisClient.set(prefixedKey, value);
    if (this.expirationInSec) this.redisClient.expire(prefixedKey, this.expirationInSec);
    
    return true;
  }

  async get(key: string): Promise<any> {
    const prefixedKey = this.prefixedKey(key);
    const strResp = await this.redisClient.get(prefixedKey);
    if (strResp) {
      return JSON.parse(strResp);
    }
  }

  async delete(key: string): Promise<boolean> {
    const prefixedKey = this.prefixedKey(key);
    await this.redisClient.del(prefixedKey);
    return true;
  }

  flush(): boolean{
    this.redisClient.flushdb();
    return true;
  }
}