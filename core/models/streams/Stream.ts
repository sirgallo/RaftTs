import {  RedisServiceType } from '@core/models/dataAccess/Redis';


//  many stream ops return arrays of arrays where inner array is [ entryId, messages ]
type EntryId = string;
export type EntryResponse = [ EntryId, string[] ];

//  initialization opts
export interface StreamOpts {
  id: string;
  service: RedisServiceType;
  streamKeyPrefix: string;
}

//  XADD 
export interface XAddOpts {
  MAXLEN?: number;
  exactLength?: boolean;
}

//  XREAD
interface XBLOCK {
  BLOCK: number;
}
interface XCOUNT {
  COUNT: number;
}
interface XCOMBO extends XBLOCK, XCOUNT {}

export type XReadOpts = XCOMBO | XBLOCK | XCOUNT;

type StreamKey = string;
//  reads return list of streams searched with sub array of entries w/ messages ==> [ streamId, [ entryId, messages[] ][] ][]
export type ReadResponse = [ StreamKey, EntryResponse[] ][];

//  XRANGE
export interface XRangeOpts {
  start: string | '-';
  end: string | '+';
  exclusiveRange?: boolean;
  COUNT?: number;
}

//  XINFO
export type StreamInfoResponse = [ 
  'length',            
  string,             
  'radix-tree-keys',    
  string,            
  'radix-tree-nodes', 
  string, 
  'last-generated-id',
  string, 
  'groups', 
  string, 
  'first-entry',
  EntryResponse, 
  'last-entry', 
  EntryResponse 
];

//  XINFO GROUP
export type GroupInfoResponse = [
  'name',
  string,
  'consumers',
  string,
  'pending',
  string,
  'last-delivered-id',
  string
];

//  XPENDING
export type PendingResponse = [
  string,                 // messageId
  string,                 // consumerName
  string,                 // minIdleTime
  string                  // timesClaimed
];

//  when trimming a stream, do we want to start at last delivered or last acknowledged?
export type LastIdStart = 'lastDelivered' | 'lastAcknowledged';
export interface TrimFromLastIdOpts {
  maxLength: number;
  lastIdStartPoint: LastIdStart;
  countPerPage: number;
}

export type XPendingOpts = Pick<XRangeOpts, 'start' | 'end' | 'COUNT'>;
export interface ProcessPendingOpts extends Required<XPendingOpts> {
  minIdleTime: number;
}

export interface ConsumerGroupOpts {
  readOpts: XReadOpts;
  pendingOpts: ProcessPendingOpts;
  trimOpts?: TrimFromLastIdOpts;
}


//=============== helpers


//  parse nested json from string
export const parseNested = (str: string) => {
  try {
    return JSON.parse(str, (_, val) => {
      if (typeof val === 'string') return parseNested(val);
      return val;
    });
  } catch (err) { return str; }
};

//  stringify json
export const stringifyIfNeeded = (message: any): string => {
  try {
    if (typeof(message) !== 'string') {
      const parsed = JSON.stringify(message);
      if (parsed !== '{}') { return parsed; }
    }

    return message;
  } catch (e) { return message; }
};

//  transform obj { [field: string]: any } into [ field1, value1, field2, value2, ... fieldN, valueN ]
export const spreadFields = (val: any) => {
  const fieldValuePairs = [];
  if (typeof(val) === 'object') {
    Object.keys(val).forEach( key => {
      const parsedPair = [ key, stringifyIfNeeded(val[key]) ];
      fieldValuePairs.push(...parsedPair);
    });
  } else { fieldValuePairs.push(...['message', val]); }     // this is a default if incoming message is a string, will set a default field

  return fieldValuePairs;
};

export const genFullKey = (prefix: string, key: string): string => `${prefix}-${key}`;
export const genLastAcknowledgedKey = (consumerGroup: string) => `last_acknowledged:${consumerGroup}`;


//=============== consts


export const LAST_ACKNOWLEDGED_FIELD = 'last_acknowledged';

//  since responses for info are arrays of [field, value], get indexes for values
export const StreamInfoValueIndexes = {
  length: 1,
  radixTreeKeys: 3,
  radixTreeNodes: 5,
  lastGeneratedId: 7,
  groups: 9,
  firstEntry: 11,
  lastEntry: 13
};

export const GroupInfoValueIndexes = {
  name: 1,
  consumers: 3,
  pending: 5,
  lastDelivered: 7
};

//  pending is [ string, string, string, string ]
export const PendingValueIndexes = {
  messageId: 0,
  consumerName: 1,
  minIdleType: 2,
  timesClaimed: 3
};