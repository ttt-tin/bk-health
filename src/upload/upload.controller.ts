import { Body, Controller, Get, Query } from '@nestjs/common';
import { UploadS3Service } from './upload-s3.service';

@Controller('upload')
export class UploadController {
  constructor(private readonly uploadService: UploadS3Service) {}

  @Get()
  async runUpload(
    @Query('bucketName') bucketName: string,
    @Query('folderPath') folderPath: string,
  ): Promise<any> {
    try {
        await this.uploadService.uploadAllFilesCSVInFolder(folderPath, './', bucketName);
        return true;
    }
    catch (error) {
        return error.message;
    }
  }
}
