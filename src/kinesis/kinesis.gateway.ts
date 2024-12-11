import { Injectable } from '@nestjs/common';
import { SubscribeMessage, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { KinesisService } from './kinesis.service';

@WebSocketGateway({
  cors: {
    origin: '*',
    credentials: true,
  },
})
@Injectable()
export class KinesisGateway {
  @WebSocketServer() server: Server;

  constructor(private readonly kinesisService: KinesisService) {}

  @SubscribeMessage('sendData')
  async handleSendData(client: Socket, payload: { data: Record<string, any>; partitionKey: string }) {
    if (!payload?.data || !payload?.partitionKey) {
      client.emit('dataSent', {
        status: 'error',
        message: 'Invalid payload: data and partitionKey are required',
      });
      return;
    }

    try {
      console.log('Sending data to Kinesis:', payload);
      await this.kinesisService.sendDataToKinesis(payload.data, payload.partitionKey);

      client.emit('dataSent', { status: 'success', message: 'Data sent to Kinesis successfully!' });
      this.server.emit('broadcast', { message: 'New data sent to Kinesis' }); // Broadcast to all clients
    } catch (error) {
      console.error('Error sending data to Kinesis:', error);
      client.emit('dataSent', {
        status: 'error',
        message: error.message || 'Failed to send data to Kinesis',
      });
    }
  }
}
