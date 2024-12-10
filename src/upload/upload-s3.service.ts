import { Injectable } from '@nestjs/common';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import * as fs from 'fs';
import * as path from 'path';

@Injectable()
export class UploadS3Service {
  private s3: S3Client;

  constructor() {
    this.s3 = new S3Client({
      region: process.env.AWS_S3_REGION,
      credentials: {
        accessKeyId: process.env.AWS_S3_ACCESS_KEY,
        secretAccessKey: process.env.AWS_S3_SECRET_KEY,
      },
    });
  }

  async uploadFile(filePath: string, s3Key: string): Promise<void> {
    const fileStream = fs.createReadStream(filePath);

    const params = {
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: s3Key,
      Body: fileStream,
    };

    try {
      const command = new PutObjectCommand(params);
      await this.s3.send(command);
      console.log(`File uploaded successfully: ${s3Key}`);
    } catch (error) {
      console.error('Error uploading file:', error);
    }
  }

  async uploadAllFilesInFolder(sourceFolder: string, destFolder: string): Promise<void> {
    const files = fs.readdirSync(sourceFolder);

    for (const filename of files) {
      const filePath = path.join(sourceFolder, filename);
      if (fs.statSync(filePath).isFile()) {
        const s3Key = path.join(destFolder, filename);
        await this.uploadFile(filePath, s3Key);
      }
    }
  }
}
