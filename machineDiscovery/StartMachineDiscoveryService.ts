import { InitMachineDiscoveryService } from './InitMachineDiscoveryService';
import { serverConfiguration } from '../ServerConfigurations';


const server = new InitMachineDiscoveryService(
  serverConfiguration.basePath,
  { 
    port: '8000', 
    protocol: 'tcp', 
    queueEventName: 'machineDiscoveryEvent' 
  },
  { 
    name: serverConfiguration.systems.raftNode.name,
    port: serverConfiguration.systems.raftNode.port,
    version: serverConfiguration.systems.raftNode.version,
    numOfCpus: serverConfiguration.systems.raftNode.numOfCpus
  }
);

try {
  server.startServer();
} catch (err) { console.log(err); }