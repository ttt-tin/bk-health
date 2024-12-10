import { Injectable } from '@nestjs/common';
import { SubscribeMessage, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { KinesisService } from './kinesis.service';

@WebSocketGateway()
@Injectable()
export class KinesisGateway {
    @WebSocketServer() server: Server;

    constructor(private readonly kinesisService: KinesisService) {}

    @SubscribeMessage('sendData')
    async handleSendData(client: Socket, payload: { data: Record<string, any>; partitionKey: string }) {
        try {
            await this.kinesisService.sendDataToKinesis(payload.data, payload.partitionKey);

            client.emit('dataSent', { status: 'success', message: 'Data sent to Kinesis successfully!' });
        } catch (error) {
            client.emit('dataSent', { status: 'error', message: 'Failed to send data to Kinesis' });
        }
    }
}
