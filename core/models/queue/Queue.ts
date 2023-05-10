import { RedisOptions } from 'ioredis';


export interface QueueOpts {
  queueName: string;
  redisOpts?: RedisOptions;
}

export type BPOPResp = [
  string,          // queue name
  string           // element
];