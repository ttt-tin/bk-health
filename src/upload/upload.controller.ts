import { Body, Controller, Get, Post, Query, UseInterceptors, UploadedFile } from '@nestjs/common';
import { Express } from 'express';
import { UploadS3Service } from './upload-s3.service';
import { FileInterceptor } from '@nestjs/platform-express';

@Controller('upload')
export class UploadController {
  constructor(private readonly uploadService: UploadS3Service) { }

  @Get()
  async runUpload(
    @Query('bucketName') bucketName: string,
    @Query('folderPath') folderPath: string,
  ): Promise<any> {
    try {
      await this.uploadService.uploadAllFilesCSVInFolder(
        folderPath,
        './',
        bucketName,
      );
      return true;
    } catch (error) {
      return error.message;
    }
  }

  @Post('file')
  @UseInterceptors(FileInterceptor('file'))
  async uploadFileForTextract(
    @UploadedFile() file: Express.Multer.File,
  ): Promise<any> {
    try {
      const bucketName = process.env.AWS_S3_BUCKET_NAME;
      const folderPath = 'unstructure';  // Define folder path for S3
      if (!file) {
        throw new Error('File upload failed, no file received.');
      }

      const fileKey = await this.uploadService.uploadFileForTextract(file, bucketName, folderPath);
      return { message: 'File uploaded successfully', fileKey };
    } catch (error) {
      return { message: error.message };
    }
  }
}
