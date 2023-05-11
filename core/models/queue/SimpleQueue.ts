import { LinkedNode, HashString } from '@core/models/infrastructure/Mq';


export type SimpleQueue = Record<HashString, LinkedNode>;
export type LinkedNodeData = Pick<LinkedNode, 'value' | 'timestamp'>

export interface SimpleQueueMethods {
  enqueue(insertValue: any): void;
  dequeue(): any;
  peek(): LinkedNodeData;
  all(): SimpleQueue; 
}