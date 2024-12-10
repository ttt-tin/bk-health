import { Injectable } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { UploadS3Service } from './upload-s3.service';

@Injectable()
export class CronUploadService {
  constructor(private readonly uploadS3Service: UploadS3Service) {}

  @Cron('0 0 * * *')
  async handleCron() {
    console.log('Run at 00h00 every day');
    const sourceFolder = process.env.SOURCE_FOLDER;
    const destFolder = process.env.DEST_FOLDER;

    console.log('Starting file upload to S3...');
    await this.uploadS3Service.uploadAllFilesInFolder(sourceFolder, destFolder);
    console.log('File upload completed.');
  }

  @Cron('* * * * *')
  async handleMinuteCron() {
    console.log('Run every minute');
    
  }
}
