export interface MqOpts {
  domain: string;
  port: string;
  topic: string;
}

export interface InternalJobQueueMessage {
  jobId: string;
  header: string;
  body: SockRequest;
}

export interface SockRequest {
  message: any;
}

export interface InternalLivelinessResponse {
  node: string;
  job: string;
  message: any;
  status: MachineStatus;
  lifeCycle?: LifeCycle;
}

export interface HeartBeatMethods {
  heartbeat: (machineId: string) => Promise<void>;
}

export interface AvailableMachine extends HeartBeatMethods {
  status: MachineStatus;
  validated: Date;
  connAttempts: number;
}

export interface HeartBeat {
  routerId: string;
  healthy: Liveliness;
  status: MachineStatus;
}

export interface LinkedNode {
  id: HashString;
  next: HashString;
  value: NodeValue;
  timestamp: Date;
}

export type HashString = string;
export type NodeValue = any;
export type Liveliness = 'Alive' | 'Dead';
export type MachineTypes = 'Client' | 'Worker' | 'Replica';
export type MachineStatus = 'Ready' | 'Busy';
export type LifeCycle = 'Not Started' | 'In Queue' | 'In Progress' | 'Finished' | 'Failed';

export type KnownMachinesMap = Record<string, AvailableMachine>;