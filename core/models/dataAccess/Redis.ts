import { RedisOptions } from 'ioredis';


export const DB_INDEX_OFFSETS = {
  default: 0,
  streams: 20
};

export const defaultRedisServiceTypesList = <const> [
  'MemCache',
  'Queue'
];

export const streamRedisServiceTypesList = <const> [
  'Streams'
];

export type RedisServiceType = typeof defaultRedisServiceTypesList[number] 
  | typeof streamRedisServiceTypesList[number];

export const DEFAULT_OPTS: RedisOptions = {
  port: 6379,
  host: 'localhost',
  connectTimeout: 10000
}