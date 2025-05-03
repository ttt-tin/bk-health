import { Module } from "@nestjs/common";
import { AppController } from "./app.controller";
import { AppService } from "./app.service";
import { ConstraintModule } from "./constraint/constraint.module";
import { PythonModule } from "./python/python.module";
import { UploadModule } from "./upload/upload.module";
import { KinesisModule } from "./kinesis/kinesis.module";
import { TypeOrmModule } from "@nestjs/typeorm";
import { MappingModule } from "./mapping/mapping.module";
import * as dotenv from "dotenv";
import { AthenaModule } from "./athena/athena..module";
import { HistoryModule } from "./history/history.module";
import { ExternalVolumeModule } from "./external-volume/external-volume.module";
import { S3Module } from "./s3/s3.module";
import { RelationshipModule } from "./relation/relation.module";
import { ExplorerModule } from "./explorer/explorer.module";
import { MulterModule } from "@nestjs/platform-express";
import { TableColumnModule } from "./table-column/table-column.module";
import { DataSourcesModule } from "./data-source/data-sources.module";
import { NotificationModule } from "./notification/notification.module";
dotenv.config();

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: "postgres",
      host: process.env.DB_HOST,
      port: 5432,
      username: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      entities: ["dist/**/**/*.entity.js"],
      synchronize: true,
    }),
    ConstraintModule,
    PythonModule,
    UploadModule,
    KinesisModule,
    MappingModule,
    AthenaModule,
    HistoryModule,
    ExternalVolumeModule,
    S3Module,
    RelationshipModule,
    ExplorerModule,
    MulterModule.register({
      limits: { fileSize: 5 * 1024 * 1024 },
    }),
    TableColumnModule,
    DataSourcesModule,
    NotificationModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule { }
