import { Injectable } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { UploadS3Service } from './upload-s3.service';
import { KinesisService } from 'src/kinesis/kinesis.service';

@Injectable()
export class CronUploadService {
  constructor(private readonly uploadS3Service: UploadS3Service, private readonly kinesisService: KinesisService) { }

  @Cron('0 0 * * *')
  async handleCron() {
    console.log('Run at 00h00 every day');
    const sourceFolder = process.env.SOURCE_FOLDER;
    const destFolder = '';

    console.log('Starting file upload to S3...');
    await this.uploadS3Service.uploadAllFilesInFolder(sourceFolder, destFolder);
    console.log('File upload completed.');
  }

  @Cron('* * * * *')
  async handleMinuteCron() {
    // console.log('Run every minute');
  //   const userData = {
  //     userId: 'user123',
  //     userName: 'John Doe',
  //     email: 'john.doe@example.com',
  // };
  
  // const partitionKey = 'user123';
  
  // await this.kinesisService.sendDataToKinesis(userData, partitionKey);
  }
}
