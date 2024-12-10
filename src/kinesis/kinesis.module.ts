import { Module } from '@nestjs/common';
import { KinesisService } from './kinesis.service';
import { KinesisGateway } from './kinesis.gateway';

@Module({
  providers: [KinesisService, KinesisGateway],
})
export class KinesisModule {}
