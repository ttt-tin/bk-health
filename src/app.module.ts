import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ConstraintModule } from './constraint/constraint.module';
import { PythonModule } from './python/python.module';
import { UploadModule } from './upload/upload.module';
import { KinesisModule } from './kinesis/kinesis.module';

@Module({
  imports: [ConstraintModule, PythonModule, UploadModule, KinesisModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
