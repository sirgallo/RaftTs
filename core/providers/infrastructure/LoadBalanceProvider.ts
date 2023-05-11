import { InternalJobQueueMessage, InternalLivelinessResponse } from '@core/models/infrastructure/Mq';
import { SocketRouterProvider } from './SocketRouterProvider';
import { LogProvider } from '../LogProvider';


const NAME = 'Load Balance Provider';

const WORKER_EVENT = 'workerEvent';
const CLIENT_EVENT = 'clientEvent';


/*
  Custom Load Balancer

  Design:

            clients ...
                |
                |
  add to known clients if not discovered yet
            add job to queue
      distribute to known workers

  Utilizing 2 separate sockets, with the frontend socket
  relaying to the backend socket

  Worker and Client unique identifiers are stored in memory

  Workers and Clients express if they are busy or not, and if 
  available, are selected at random to either begin a new job
  or return a response
*/


export class LoadBalanceProvider {
  client: SocketRouterProvider;
  worker: SocketRouterProvider;

  private zLog = new LogProvider(NAME);
  
  constructor(
    private address: string, private protocol = 'tcp', 
    private clientPort: string, private workerPort: string
  ) {
    this.client = new SocketRouterProvider(this.clientPort, this.protocol, NAME, CLIENT_EVENT);
    this.worker = new SocketRouterProvider(this.workerPort, this.protocol, NAME, WORKER_EVENT);
  }

  //  Start both frontend and backend facing sockets and associated queues
  async run() {
    try {
      this.workerQueueOn();
      this.clientQueueOn();

      this.startClientRouter();
      this.startWorkerRouter();
    } catch (err) {
      this.zLog.error(err);
      throw err;
    }
  }

  /*
    Client facing socket that picks up requests from the gateway apis. Requests are then fed into the job pipeline
    and fed into the worker router. Any number of clients can connect to the client side.

    The load balancer saves client ids in a map and then heartbeats every system in the map to validate that it is alive.
    If the system fails, it is removed from the map of validated systems. Responses from the worker machines are routed
    to a randomly selected available client machine
  */
  async startClientRouter() {
    const clientResponseHandler = (strHeader: string, jsonBody: any) => {
      const queueEntry: InternalJobQueueMessage = {
        jobId: jsonBody?.message,
        header: strHeader,
        body: jsonBody
      }

      if (queueEntry.jobId) {
        const returnObj: InternalLivelinessResponse = {
          node: this.address,
          job: queueEntry.jobId,
          message: queueEntry.body,
          status: 'Ready',
          lifeCycle: 'In Queue'
        }

        const body = { body: returnObj }

        this.client.queue.enqueue(body);
        this.worker.queue.enqueue(queueEntry);
      }
    }
    
    await this.client.startRouter(clientResponseHandler.bind(this));
  }

  /*
    Requests on job queue are picked up by worker router and distributed to worker machines. Machines are picked 
    randomly from a pool of available machines, with status: 'Ready' .This evenly distributes work to available 
    machines. Any number of workers can connect to the worker facing load balancer.
  */
  async startWorkerRouter() {
    const workerResponseHandler = (strHeader: string, jsonBody: any) => {
      if (jsonBody.job) {
        const retEntry = { body: jsonBody }
        this.client.queue.enqueue(retEntry);
      }
    }
    
    await this.worker.startRouter(workerResponseHandler.bind(this));
  }

  //  handle randomly distributing jobs on new job event and stale jobs
  private workerQueueOn() {
    const formatMessage = (msg: InternalJobQueueMessage): string => { return JSON.stringify(msg.body); }
    this.worker.queueListener(formatMessage, 'single');
  }

  //  handle randomly distributing a response to a gateway machine
  private clientQueueOn() {
    const formatMessage = (msg: any): string => { return JSON.stringify(msg); }
    this.client.queueListener(formatMessage, 'single');
  }
}