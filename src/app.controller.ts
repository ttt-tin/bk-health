import { Controller, Get } from '@nestjs/common';
import { AppService } from './app.service';
import { UploadS3Service } from './upload/upload-s3.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService, private readonly uploadS3Service: UploadS3Service) {}

  @Get()
  async getHello(): Promise<any> {
    // return this.appService.getHello();

    const sourceFolder = process.env.SOURCE_FOLDER;
    const destFolder = process.env.DEST_FOLDER;

    console.log('Starting file upload to S3...');
    await this.uploadS3Service.uploadAllFilesInFolder(sourceFolder, destFolder);
    console.log('File upload completed.');
  }
}
