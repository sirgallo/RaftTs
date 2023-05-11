import * as Redis from 'ioredis';
import { ValueType } from 'ioredis';
import { first, last } from 'lodash';

import { LogProvider } from '@core/providers/LogProvider';
import { 
  XAddOpts, XRangeOpts, XReadOpts, TrimFromLastIdOpts, XPendingOpts,
  ReadResponse, StreamInfoResponse, EntryResponse, GroupInfoResponse,
  genFullKey, spreadFields, genLastAcknowledgedKey,
  LAST_ACKNOWLEDGED_FIELD, GroupInfoValueIndexes, StreamInfoValueIndexes, PendingResponse
} from '@core/models/streams/Stream';


/*
  Base Stream Provider

  --  provide basic redis stream commands
*/


export class BaseStreamProvider {
  constructor(protected redisClient: Redis.Redis, protected streamKeyPrefix: string, protected id: string, protected zLog: LogProvider) {}

  getInstanceId() {
    this.zLog.info(`current provider id: ${this.id}`);
    return this.id;
  }

  async exists(streamKey: string): Promise<boolean> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    const result = await this.redisClient.exists(prefixedStreamKey);
    
    return result === 1;
  }

  async clearStream(streamKey: string): Promise<boolean> {
    await this.trim(streamKey, 0, true);
    
    return true;
  }

  async info(streamKey: string): Promise<StreamInfoResponse> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    return this.redisClient.xinfo('STREAM', prefixedStreamKey);
  }

  async groupInfo(streamKey: string): Promise<GroupInfoResponse[]> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    return this.redisClient.xinfo('GROUPS', prefixedStreamKey);
  }

  async groupCreate(streamKey: string, consumerGroup: string): Promise<boolean> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    await this.redisClient.xgroup('CREATE', prefixedStreamKey, consumerGroup, '$', 'MKSTREAM');
    this.zLog.debug(`created consumer group --> ${consumerGroup}`);
    
    return true;
  }

  async groupDestroy(streamKey: string, consumerGroups: string[]): Promise<boolean> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    for (const consumerGroup of consumerGroups) {
      await this.redisClient.xgroup('DESTROY', prefixedStreamKey, consumerGroup);
    }
    
    return true;
  }

  async add(streamKey: string, val: any, opts?: XAddOpts, predefinedId = '*'): Promise<string> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    const args = spreadFields(val);
    const parsedArgs = ParseStreamArguments.parseXAddArgs(args, predefinedId, opts);

    return this.redisClient.xadd(prefixedStreamKey, ...parsedArgs);
  }

  async read(streamKey: string, opts: XReadOpts, lastId = '$'): Promise<ReadResponse> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    const parsedArgs = ParseStreamArguments.parseXReadArgs(prefixedStreamKey, lastId, opts);

    return this.redisClient.xread(...parsedArgs);
  }

  async readGroup(streamKey: string, consumerGroup: string, consumerName: string, opts: XReadOpts, lastId = '>'): Promise<ReadResponse> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    const parsedArgs = ParseStreamArguments.parseXReadGroupArgs(prefixedStreamKey, lastId, consumerGroup, consumerName, opts);
    
    return this.redisClient.xreadgroup('GROUP', parsedArgs);
  }

  async ack(streamKey: string, consumerGroup: string, id: string): Promise<boolean> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    const lastAckKey = genLastAcknowledgedKey(consumerGroup);
    
    await this.redisClient.xack(prefixedStreamKey, consumerGroup, id);
    
    //  keep track of last acknowledged since redis doesn't already do this
    //  use transaction to ensure that if key is being modified concurrently, skip
    const transaction = this.redisClient.multi();
    transaction.watch(lastAckKey);
    transaction.hset(lastAckKey, LAST_ACKNOWLEDGED_FIELD, id);

    await transaction.exec().catch(err => { this.zLog.error(`unable to set last acknowledged ${err}`); });

    return true;
  }

  async claim(streamKey: string, consumerGroup: string, consumerName: string, ids: string[], minIdleTime?: number): Promise<EntryResponse[]> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    return this.redisClient.xclaim(prefixedStreamKey, ParseStreamArguments.parseXClaimArgs(consumerGroup, consumerName, ids, minIdleTime));
  }

  async pending(streamKey:string, consumerGroup: string, opts: XPendingOpts): Promise<PendingResponse[]> {
    const prefixedKey = genFullKey(this.streamKeyPrefix, streamKey);
    const count = await (async () => {
      if (opts?.COUNT) return opts.COUNT;
      else { 
        const groupInfos = await this.groupInfo(streamKey);
        if (! groupInfos || groupInfos.length < 1) return 0;

        const groupInfo = groupInfos.filter(group => group[GroupInfoValueIndexes.name] === consumerGroup)[0];
        const pending = groupInfo[GroupInfoValueIndexes.pending];
        return parseInt(pending);
      }
    })();

    return this.redisClient.xpending(prefixedKey, consumerGroup, opts.start, opts.end, count);
  }

  async getLastAcknowledgedId(consumerGroup: string): Promise<string> {
    const lastAckKey = genLastAcknowledgedKey(consumerGroup);
    return this.redisClient.hget(lastAckKey, LAST_ACKNOWLEDGED_FIELD);
  }

  async getLastDeliveredId(streamKey: string, consumerGroup: string): Promise<string> {
    const groupInfo = await this.groupInfo(streamKey);
    const currentGroupInfo = first(groupInfo.filter(group => group[GroupInfoValueIndexes.name] === consumerGroup));

    return currentGroupInfo[GroupInfoValueIndexes.lastDelivered];
  }

  async range(streamKey: string, opts: XRangeOpts): Promise<EntryResponse[]> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    const parsedArgs = ParseStreamArguments.parseXRangeArgs(opts);
    
    return this.redisClient.xrange(prefixedStreamKey, ...parsedArgs);
  }

  async len(streamKey: string): Promise<number> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    return this.redisClient.xlen(prefixedStreamKey);
  }

  async del(streamKey: string, ids: string[]): Promise<boolean> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    await this.redisClient.xdel(prefixedStreamKey, ...ids);
    
    return true;
  }

  //  to delete in a range of values, need to paginate over xrange so we don't run into memory issues
  async paginateDelRange(streamKey: string, opts: XRangeOpts): Promise<boolean> {
    const entries = await this.range(streamKey, opts);
    if (! entries || entries.length < 1) return true;
    
    const entryIdsToDelete = ( (): string[] => {
      if (entries.length < opts?.COUNT) return entries.map(entry => entry[0]);
      return entries.slice(0, -1).map(entry => entry[0]);          //  up to last id
    })();

    await this.del(streamKey, entryIdsToDelete);

    if (entries.length < opts?.COUNT) { 
      this.zLog.info(`paginated delete for ${streamKey} completed.`);
      return true;
    }

    const lastEntryId = last(entries)[0];
    await this.paginateDelRange(streamKey, { start: lastEntryId, end: opts.end, COUNT: opts.COUNT });             //  recursively paginate up to end id
  }

  async trim(streamKey: string, maxLength: number, exactLength = false): Promise<boolean> {
    const prefixedStreamKey = genFullKey(this.streamKeyPrefix, streamKey);
    await this.redisClient.xtrim(prefixedStreamKey, ...ParseStreamArguments.parseXTrimArgs(maxLength, exactLength));

    return true;
  }

  async trimFromLastId(streamKey: string, consumerGroup: string, opts: TrimFromLastIdOpts): Promise<boolean> {
    const streamLength = await this.len(streamKey) || 0;
    if (streamLength > opts.maxLength) {
      const maxIdToTrim = await (async () => {
        if (opts.lastIdStartPoint === 'lastAcknowledged') return await this.getLastAcknowledgedId(consumerGroup);
        if (opts.lastIdStartPoint === 'lastDelivered') return await this.getLastDeliveredId(streamKey, consumerGroup);
      })();

      const firstEntryId = (await this.info(streamKey))[StreamInfoValueIndexes.firstEntry][0];
      await this.paginateDelRange(streamKey, { start: firstEntryId, end: maxIdToTrim, COUNT: opts.countPerPage });               
    }

    return true;
  }
}


//=============== parse args for redis opts from JSON: JSON --> array


//  [ ] denotes optional fields, < > denotes or
class ParseStreamArguments {

  //  MAXLEN [ ~ ] threshold < * | id > field1, value1, ... fieldN, valueN
  static parseXAddArgs = (args: any[], id: string, opts?: XAddOpts): ValueType[] => {
    const parsed = [];
    if (opts) {
      if (opts?.MAXLEN) {
        parsed.push('MAXLEN');
        if (! opts?.exactLength) parsed.push('~');    //  in cases where we do not need exact length. This is faster
        parsed.push(opts?.MAXLEN);
      }
    }
    parsed.push(id, ...args);

    return parsed;
  };

  // [ COUNT count ] [ BlOCK millisecs ] STREAMS key id
  static parseXReadArgs = (streamKey: string, id: string, opts: XReadOpts): ValueType[] => {
    const parsedArgs = [];
    Object.keys(opts).forEach( key => {
      const currArg = [ key, opts[key] ];
      parsedArgs.push(...currArg);
    });
  
    const streams = [ 'STREAMS', streamKey, id ];
    parsedArgs.push(...streams);
  
    return parsedArgs;
  };

  //  consumerGroup consumerName [ COUNT count ] [ BLOCK millisecs ] STREAMS key id
  static parseXReadGroupArgs = (streamKey: string, id: string, consumerGroup: string, consumerName: string, opts: XReadOpts): ValueType[] => {
    const parsedArgs = [];
    const group = [ consumerGroup, consumerName ];
    parsedArgs.push(...group);
    parsedArgs.push(...ParseStreamArguments.parseXReadArgs(streamKey, id, opts));

    return parsedArgs;
  };
  
  //  key < - | start > < + | end > [ COUNT count ]
  static parseXRangeArgs = (opts: XRangeOpts): ValueType[] => {
    const parsedArgs = [];
  
    if (opts?.exclusiveRange) {
      const prefixStart = (start: string) => `(${start}`;
      parsedArgs.push(prefixStart, opts.end);
    } else { parsedArgs.push(opts.start, opts.end); }
  
    if (opts?.COUNT) { 
      const optionalCount = [ 'COUNT', opts?.COUNT ];
      parsedArgs.push(...optionalCount);
    }
  
    return parsedArgs;
  };
  
  //  key MAXLEN [ ~ ] threshold 
  static parseXTrimArgs = (maxLength: number, exactLength?: boolean): ValueType[] => {
    const parsed = [];
    parsed.push('MAXLEN');
    if (! exactLength) parsed.push('~');
    parsed.push(maxLength);
  
    return parsed;
  };

  //  consumerGroup consumerName minIdleTime ...ids
  static parseXClaimArgs = (consumerGroup: string, consumerName: string, ids: string[], minIdleTime?: number): ValueType[] => {
    const parsed: any[] = [ consumerGroup, consumerName ];
    if (minIdleTime) parsed.push(minIdleTime);
    parsed.push(...ids);

    return parsed;
  };
}