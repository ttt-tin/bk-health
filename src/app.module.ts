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
import { RelationshipModule } from "./relation/relation.module";
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
    RelationshipModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule { }
