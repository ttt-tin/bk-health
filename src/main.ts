import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as dotenv from 'dotenv';
import cors from 'cors';
import { MySocketIoAdapter } from './socket-io-adapter';

async function bootstrap() {
  dotenv.config();
  const app = await NestFactory.create(AppModule);

  // Apply CORS settings to HTTP
  // app.use(cors({
  //   origin: 'http://127.0.0.1:5500',
  //   methods: ['GET', 'POST', 'PUT', 'DELETE'],
  //   credentials: true,
  // }));

  app.enableCors({
    origin: '*', // Cho phép tất cả các nguồn
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE', // Cho phép các phương thức này
    allowedHeaders: 'Content-Type, Authorization', // Các header cho phép
    credentials: true, // Cho phép cookie cross-origin (nếu cần)
  });

  console.log(process.env.DB_HOST)

  // Use the custom Socket.IO adapter to handle WebSocket CORS
  app.useWebSocketAdapter(new MySocketIoAdapter(app));

  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
