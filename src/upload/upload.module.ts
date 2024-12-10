import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { UploadService } from './upload.service';
import { CronUploadService } from './cron-upload.service';

@Module({
  imports: [ScheduleModule.forRoot()],
  providers: [UploadService, CronUploadService],
})
export class UploadModule {}
