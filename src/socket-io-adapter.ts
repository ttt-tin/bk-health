import { IoAdapter } from '@nestjs/platform-socket.io';
import { createServer } from 'http';
import { Server } from 'socket.io';

export class MySocketIoAdapter extends IoAdapter {
  createIOServer(port: number, options?: any): any {
    const server = super.createIOServer(port, options);

    const io = new Server(server, {
      cors: {
        origin: '*',
        methods: ['GET', 'POST'],
        allowedHeaders: ['Content-Type'],
        credentials: true,
      },
    });

    return io;
  }
}
