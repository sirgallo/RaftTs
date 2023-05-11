import { chunk, last } from 'lodash';

import { LogProvider } from '@core/providers/LogProvider';
import { RedisProvider } from '@core/providers/dataAccess/RedisProvider';
import { 
  StreamOpts, ConsumerGroupOpts, ProcessPendingOpts,
  ReadResponse, EntryResponse,
  GroupInfoValueIndexes, PendingValueIndexes, parseNested
} from '@core/models/streams/Stream';
import { BaseStreamProvider } from '@core/providers/streams/BaseStreamProvider';


/*
  Base Stream Consumer Provider -->

  extend this to create new consumers

  -- creates a an instance of a redis stream consumer, using consumer groups
  -- handles processing incoming messages on defined stream

  abstract processor: implement the processor for incoming messages
*/


export abstract class BaseStreamConsumerProvider extends BaseStreamProvider {
  protected name: string;
  constructor(opts: StreamOpts, name: string) {
    const redisClient = new RedisProvider().getClient(opts.service);
    const log = new LogProvider(name);

    super(redisClient, opts.streamKeyPrefix, opts.id, log);
    this.name = name;
  }

  abstract startConsumer(): void;
  abstract processor(entry: any): Promise<boolean>;

  //  create a listener that reads and processes messages from a stream denoted by a consumer group
  async consumeGroup(streamKey: string, consumerGroup: string, opts: ConsumerGroupOpts) {
    //  ingest incoming messages recursively --> lastId will always be the '>' operator since this denotes id of last delivered message
    const listenHandler = async (lastId = '>') => {
      const res: ReadResponse = await this.readGroup(streamKey, consumerGroup, this.id, opts.readOpts, lastId);    // blocks until a new incoming message enters stream
      if (res) { 
        const [ _, messages ] = res[0];
        await this.handleMessage(streamKey, consumerGroup, messages);
      }

      if (opts?.trimOpts) await this.trimFromLastId(streamKey, consumerGroup, opts.trimOpts);
      await listenHandler();
    };

    try {
      this.zLog.info(`starting consumer for key ${streamKey}`);

      const exists = await this.exists(streamKey);

      if (! exists) { 
        this.zLog.info(`stream does not exist, creating group and empty stream for ${streamKey}`);
        await this.groupCreate(streamKey, consumerGroup);
      } else {
        const streamInfo = await this.groupInfo(streamKey);
        if (streamInfo?.length < 1) { 
          this.zLog.info(`consumer group does not exist, creating consumer group for', ${streamKey}`);
          await this.groupCreate(streamKey, consumerGroup);
        } else {
          const consumerGroups: string[] = streamInfo.map(group => group[GroupInfoValueIndexes.name]);
          if (! consumerGroups.includes(consumerGroup)) { 
            this.zLog.info(`consumer group does not exist in consumer groups for stream ${streamKey}...creating`);
            await this.groupCreate(streamKey, consumerGroup);
          } else { await this.processPending(streamKey, consumerGroup, opts.pendingOpts); }
        }
      }

      this.zLog.info(`starting from last delivered message in stream for ${streamKey}`);
      await listenHandler();
    } catch (err) {
      this.zLog.error((err as Error).message);
      throw err;
    }
  }

  //  attempt to process messages that have been delivered but never acknowledged, paginate to avoid memory issues if large set of pending
  private async processPending(streamKey: string, consumerGroup: string, opts: ProcessPendingOpts): Promise<boolean> {
    const pendingEntries = await this.pending(streamKey, consumerGroup, { start: opts.start, end: opts.end, COUNT: opts.COUNT });
    if (! pendingEntries || pendingEntries.length < 1) {
      this.zLog.info(`no pending entries to process on consumer group ${consumerGroup}`);
      return true;
    }
    
    const entryIds = ( (): string[] => { 
      if (pendingEntries.length < opts.COUNT) return pendingEntries.map(pending => pending[PendingValueIndexes.messageId]);
      return pendingEntries.slice(0, -1).map(pending => pending[PendingValueIndexes.messageId]);              // up to last id
    })();
    
    const claimedMessages = await this.claim(streamKey, consumerGroup, this.id, entryIds, opts.minIdleTime);
    await this.handleMessage(streamKey, consumerGroup, claimedMessages);
    this.zLog.info(`${claimedMessages.length} pending messages processed.`);

    if (pendingEntries.length < opts.COUNT) { 
      this.zLog.info(`all pending entries claimed and processed for consumer group ${consumerGroup} on consumer with ${this.id}`);
      return true;
    }

    const lastEntryId = last(pendingEntries)[0];
    await this.processPending(streamKey, consumerGroup, { start: lastEntryId, end: opts.end, COUNT: opts.COUNT, minIdleTime: opts.minIdleTime });
  }

  //  handleMessage: parse response into json --> process response --> acknowledge message
  private async handleMessage(streamKey: string, consumerGroup: string, res: EntryResponse[]): Promise<boolean> {
    for (const [ entryId, entry ] of res) {
      const parsedMessage = this.parseReadResponse(entry);
      await this.processor(parsedMessage);                                         //  perform operation on the parsed message
      await this.ack(streamKey, consumerGroup, entryId);                           //  acknowledge message after processing -- last delivered set before acknowledge
    }

    return true;
  }

  private parseReadResponse(redisReadResp: string[]) {
    //  incoming array is [ field1, value1, field2, value2, ... fieldN, valueN ]
    const chunkedResp = chunk(redisReadResp, 2);
    const retObj = {};
    chunkedResp.forEach( (chunk: [string, string]) => retObj[chunk[0]] = parseNested(chunk[1]));
  
    return retObj;
  }
}