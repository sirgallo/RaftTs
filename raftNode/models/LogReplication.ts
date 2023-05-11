/*
  term --> when entry was received by leader
  command --> operation on state machine
*/
export interface LogEntry {
  term: number;
  command: string;
}

/*
  term --> leader’s term
  leaderId --> so follower can redirect clients
  prevLogIndex --> index of log entry immediately preceding new ones
  prevLogTerm --> term of prevLogIndex entry
  entries[] --> log entries to store (empty for heartbeat)
*/
export interface AppendEntriesRPC {
  term: number;
  leaderId: string;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

export interface AppendEntriesRPCResponse {
  term: number;
  success: boolean;
}