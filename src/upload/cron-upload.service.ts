import { Injectable } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { UploadService } from './upload.service';

@Injectable()
export class CronUploadService {
  constructor(private readonly uploadService: UploadService) {}

  @Cron('0 0 * * *')
  async handleCron() {
    const sourceFolder = process.env.SOURCE_FOLDER;
    const destFolder = process.env.DEST_FOLDER;

    console.log('Starting file upload to S3...');
    await this.uploadService.uploadAllFilesInFolder(sourceFolder, destFolder);
    console.log('File upload completed.');
  }

  @Cron('* * * * *')
  async handleMinuteCron() {
    const sourceFolder = process.env.SOURCE_FOLDER;
    const destFolder = process.env.DEST_FOLDER;

    console.log('Starting file upload to S3...');
    await this.uploadService.uploadAllFilesInFolder(sourceFolder, destFolder);
    console.log('File upload completed.');
  }
}
