import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ConstraintModule } from './constraint/constraint.module';
import { PythonModule } from './python/python.module';
import { UploadModule } from './upload/upload.module';
import { KinesisModule } from './kinesis/kinesis.module';
import { TypeOrmModule } from '@nestjs/typeorm';
import { MappingModule } from './mapping/mapping.module';
import * as dotenv from 'dotenv';
dotenv.config();

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: 'web-service-db.cjgiasm6wf6c.ap-southeast-2.rds.amazonaws.com',
      port: 5432,
      username: 'postgres',
      password: 'bkhealth1703',
      database: 'web_service_db',
      entities: ['dist/**/**/*.entity.js'],
      synchronize: true,
      ssl: { rejectUnauthorized: false },
    }),
    ConstraintModule, PythonModule, UploadModule, KinesisModule, MappingModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule { }
