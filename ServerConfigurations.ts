import { 
  ServerConfigMapping,
  ServerConfiguration,
  IServerConfiguration
} from '@core/baseServer/core/models/ServerConfiguration';


export const systems: ServerConfiguration<Record<string, IServerConfiguration>> = {
  raftNode: {
    port: 1098,
    name: 'Raft Node API',
    numOfCpus: 1,
    version: '0.0.1-dev'
  }
}

export const serverConfiguration: ServerConfigMapping = {
  basePath: '/raftts_v1',
  systems
};