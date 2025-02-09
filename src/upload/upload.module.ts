import { Module } from "@nestjs/common";
import { ScheduleModule } from "@nestjs/schedule";
import { UploadS3Service } from "./upload-s3.service";
import { CronUploadService } from "./cron-upload.service";
import { KinesisService } from "src/kinesis/kinesis.service";
import { UploadController } from "./upload.controller";
import { AthenaModule } from "src/athena/athena..module";

@Module({
  imports: [ScheduleModule.forRoot(), AthenaModule],
  controllers: [UploadController],
  providers: [UploadS3Service, CronUploadService, KinesisService],
  exports: [UploadS3Service],
})
export class UploadModule {}
