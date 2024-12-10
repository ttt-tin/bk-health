import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { UploadS3Service } from './upload-s3.service';
import { CronUploadService } from './cron-upload.service';

@Module({
  imports: [ScheduleModule.forRoot()],
  providers: [UploadS3Service, CronUploadService],
})
export class UploadModule {}
