export type NodeRole = 'Leader' | 'Candidate' | 'Follower';

export interface NodeAttributes {
  currentTerm: number;
  votedFor: string;
}