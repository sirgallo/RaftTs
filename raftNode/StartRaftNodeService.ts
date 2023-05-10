import { InitRaftNodeService } from '@raftNode/InitRaftNodeService';
import { serverConfiguration } from '../ServerConfigurations';

const server = new InitRaftNodeService(
  serverConfiguration.basePath,
  serverConfiguration.systems.raftNode.name,
  serverConfiguration.systems.raftNode.port,
  serverConfiguration.systems.raftNode.version,
  serverConfiguration.systems.raftNode.numOfCpus
);

try {
  server.startServer();
} catch (err) { console.log(err); }