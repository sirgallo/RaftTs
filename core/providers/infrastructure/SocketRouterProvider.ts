import { Router } from 'zeromq';
import { randomUUID } from 'crypto';

import { LogProvider } from '@core/providers/LogProvider';
import { KnownMachinesMap, HeartBeatMethods } from '@core/models/infrastructure/Mq';
import { setIntervalQueue, sleep, toMs } from '@core/utils/Utils';
import { cryptoOptions } from '@core/crypto/CryptoOptions';
import { SimpleQueueProvider } from '@core/providers/queue/SimpleQueueProvider';


const MAXRETRIES = 5;

const ONSTARTUP = 45; // in sec
const TIMEOUT = 500; // in ms
const INTERVAL = 30; // in sec
const strEncoding = 'utf-8';

type HandleSockResponse = (strHeader: string, jsonBody: any) => void;
type FormatMessage = (msg: any) => string;

export class SocketRouterProvider implements HeartBeatMethods {
  sock: Router;
  queue: SimpleQueueProvider;
  knownMachinesMap: KnownMachinesMap;
 
  private shouldHeartbeat = true;
  private knownMachines: Set<string>;
  private zLog: LogProvider;

  constructor(private port: string, private protocol = 'tcp', private name: string, private queueEventName: string, sock?: Router) {
    this.zLog = new LogProvider(this.name);
    this.queue = new SimpleQueueProvider(this.queueEventName);

    if (sock) this.sock = sock;
  }

  setHeartBeatStatus(shouldHeartBeat: boolean) {
    this.shouldHeartbeat = shouldHeartBeat;
  }

  setMachineMap(mapping: KnownMachinesMap) {
    this.knownMachinesMap = mapping;
    const machines = Object.keys(mapping); 
    this.knownMachines = new Set(machines);
  }

  async startRouter(sockResponseHandler?: HandleSockResponse) {
    if (! this.sock) {
      this.sock.routingId = randomUUID(cryptoOptions);
      await this.sock.bind(`${this.protocol}://*:${this.port}`);
    }

    for await (const [ header, body ] of this.sock) {
      const strHeader = header.toString(strEncoding);
      const jsonBody = JSON.parse(body.toString(strEncoding));
      this.zLog.info(body.toString(strEncoding));
      this.knownMachines.add(strHeader);

      if (! this.knownMachinesMap[strHeader]) { 
        this.knownMachinesMap[strHeader] = {
          status: 'Ready',
          validated: new Date(),
          heartbeat: this.heartbeat.bind(this),
          connAttempts: 0
        }

        this.knownMachinesMap[strHeader].heartbeat(strHeader);
      } else {
        this.knownMachinesMap[strHeader].status = jsonBody?.status || 'Ready';
        this.knownMachinesMap[strHeader].validated = new Date();
        this.knownMachinesMap[strHeader].connAttempts = 0;
      }

      if (sockResponseHandler) await sockResponseHandler(strHeader, jsonBody);
    }
  }

  queueListener(formatMessage: FormatMessage, socketSendType: 'broadcast' | 'single', timeOut?: number) {
    this.queue.queueUpdate.on(this.queue.eventName, async () => {
      if (this.queue.length > 0) {
        try {
          if (socketSendType === 'broadcast') this.broadcastMessage(formatMessage);
          else if (socketSendType === 'single') this.sendMessageToRandomMachine(formatMessage);
        } catch (err) { this.zLog.error(err); }
      }
    });

    setIntervalQueue(this.queue, timeOut);
  }

  /*
    Heartbeat:

      select all machines on the known machines dictionary and check if the validation date is within the 
      timeframe.

      A heartbeat is sent to all machines and a response is returned.

      The response is handled in the socket event listener for both the client and backend facing sockets

      On response, the map is updated to reflect the machine health
  */
  async heartbeat(machineId: string) {
    this.zLog.info(`Sleep on Startup for: ${ONSTARTUP}s`);
    await sleep(toMs.sec(ONSTARTUP));
    this.zLog.info(`Begin Heartbeating for machine ${machineId}...`);
    
    while (true) {
      if (this.shouldHeartbeat) {
        const previousConnectionAttempts = this.knownMachinesMap[machineId].connAttempts;
        if (previousConnectionAttempts > MAXRETRIES) {
          this.zLog.info(`Removing Worker Machine with Id: ${machineId}`);
          this.knownMachines.delete(machineId);
          delete this.knownMachinesMap[machineId];
  
          break;
        } else {
          this.knownMachinesMap[machineId].connAttempts++;
          const currTimeout = this.getCurrentTimeout(previousConnectionAttempts);
          
          await this.sock.send([ machineId, JSON.stringify({ heartbeat: true }) ]);
          await sleep(toMs.sec(currTimeout));
        }
      }
    }
  }

  private async broadcastMessage(formatMessage: FormatMessage): Promise<boolean> {
    const machinesArray = [ ...this.knownMachines ];
    if (machinesArray.length > 0) {
      const resp = this.queue.dequeue();
      const strBody = formatMessage(resp);

      const broadcastPromises: Promise<any>[] = []
      for (const machine of machinesArray) {
        broadcastPromises.push(this.sock.send([ machine, strBody ]));
      }

      await Promise.all(broadcastPromises);
    } else { this.zLog.warn('Waiting for available machines...'); }

    return true;
  }

  private async sendMessageToRandomMachine(formatMessage: FormatMessage): Promise<boolean> {
    const machineIndex = this.selectMachine();
    if (machineIndex) {
      const resp = this.queue.dequeue();
      const strBody = formatMessage(resp);

      // need to pass identity in first frame
      // router stores id of dealer in hash table
      await this.sock.send([ 
        [...this.knownMachines][machineIndex],
        strBody 
      ]);
    } else { this.zLog.warn('Waiting for available machines...'); }

    return true;
  }
    
  //  Get a random machine index from the available machines
  private selectMachine(): number {
    const totalAvailableMachines = Object.keys(this.knownMachinesMap)
      .map(key => {
        if (
          this.knownMachinesMap[key].status === 'Ready' 
          && ! this.outsideTimeout(this.knownMachinesMap[key].validated, INTERVAL
        )) { return key; }
      })
      .filter(el => el)
      .length;
    
    if (totalAvailableMachines > 0) {
      const randomValue = Math.random() * totalAvailableMachines;
      const roundedIndex = Math.floor(randomValue);
      
      return roundedIndex;
    }
  }
    
  private outsideTimeout(dateToTest: Date, timeout: number): boolean {
    const now = new Date();
    const pastTimeout = dateToTest.getTime() + timeout;

    if (now.getTime() > pastTimeout) return true;
    else return false;
  }

  //  calculate current timeout based on current attempt
  private getCurrentTimeout(connAttempts: number): number {
    if (connAttempts === 0) return INTERVAL;
    else return this.exponentialBackoffTimeout(connAttempts);
  }

  //  exponentially back off for each machine retry
  private exponentialBackoffTimeout(connAttempts: number): number {
    return (2 ** connAttempts * TIMEOUT) / 1000;
  }
}