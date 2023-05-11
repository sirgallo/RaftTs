import * as Redis from 'ioredis';

import { RedisProvider } from '@core/providers/dataAccess/RedisProvider';
import { LogProvider } from '@core/providers/LogProvider';
import { StreamOpts, XAddOpts } from '@core/models/streams/Stream';
import { BaseStreamProvider } from '@core/providers/streams/BaseStreamProvider';


/*
  Stream Producer Provider -->

  -- used to append messages into a Redis Stream
  -- is a singleton so only one connection needs to be made per service
*/


const NAME = 'Stream Producer Provider';

export class StreamProducerProvider extends BaseStreamProvider {
  private static singleton: StreamProducerProvider;
  private constructor(redisClient: Redis.Redis, streamKeyPrefix: string, id: string, streamLog: LogProvider) {
    super(redisClient, streamKeyPrefix, id, streamLog);
  }

  static getInstance(opts: StreamOpts) {
    if (! StreamProducerProvider.singleton) {
      const redisClient = new RedisProvider().getClient(opts.service);
      const prefix = opts.streamKeyPrefix;
      const id = opts.id;
      const log = new LogProvider(NAME);

      StreamProducerProvider.singleton = new StreamProducerProvider(redisClient, prefix, id, log);
      log.info(`created singleton stream producer client for prefix: ${prefix}`);
    }

    return StreamProducerProvider.singleton;
  }

  async produceMessage(streamKey: string, val: any, opts?: XAddOpts): Promise<boolean> {
    await this.add(streamKey, val, opts);
    return true;
  }
}