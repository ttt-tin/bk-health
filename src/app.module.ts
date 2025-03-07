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
dotenv.config();

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: "postgres",
      host: process.env.DB_HOST,
      port: 5432,
      username: "postgres",
      password: "bkhealth1703",
      database: "web_service_db_2",
      entities: ["dist/**/**/*.entity.js"],
      synchronize: true,
      ssl: { rejectUnauthorized: false },
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
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
