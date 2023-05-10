export interface RequestVoteRPC {
  term: number;
  candidateId: string;
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteRPCResponse {
  term: number;
  voteGranted: boolean;
}

export type NodeRole = 'Leader' | 'Candidate' | 'Follower';