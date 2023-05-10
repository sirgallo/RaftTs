import { Redis, RedisOptions } from 'ioredis';
import { 
  RedisServiceType, DEFAULT_OPTS, DB_INDEX_OFFSETS,
  defaultRedisServiceTypesList, streamRedisServiceTypesList
} from '@core/models/dataAccess/Redis';


export class RedisProvider {
  private clientMapping: Partial<Record<RedisServiceType, Redis>> = {};
  constructor(private opts?: RedisOptions) {}

  getClient(serviceType: RedisServiceType): Redis {
    const opts: RedisOptions = ( () => {
      if (this?.opts) return { ...this.opts, ...{ db: this.getDBIndex(serviceType) }};
      return { ...DEFAULT_OPTS, ...{ db: this.getDBIndex(serviceType) }};
    })();

    if (! this.clientMapping[serviceType]) {
      this.clientMapping[serviceType] = new Redis(opts);
    }

    return this.clientMapping[serviceType];
  }

  private getDBIndex(serviceType: RedisServiceType) {
    const defaultIndex = defaultRedisServiceTypesList.indexOf(serviceType as any);
    if (defaultIndex !== -1) return DB_INDEX_OFFSETS.default + defaultIndex;

    const streamIndex = streamRedisServiceTypesList.indexOf(serviceType as any);
    if (streamIndex !== -1) return DB_INDEX_OFFSETS.streams + streamIndex;
  }
}