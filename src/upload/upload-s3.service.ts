import { Injectable } from '@nestjs/common';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import * as fs from 'fs';
import * as path from 'path';

@Injectable()
export class UploadS3Service {
  private s3: S3Client;

  constructor() {
    this.s3 = new S3Client({
      region: process.env.AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY,
        secretAccessKey: process.env.AWS_SECRET_KEY,
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

    // Get the current date and time for partitioning
    const currentDate = new Date();
    const yyyy = currentDate.getFullYear();
    const mm = (currentDate.getMonth() + 1).toString().padStart(2, '0');
    const dd = currentDate.getDate().toString().padStart(2, '0');
    const hh = currentDate.getHours().toString().padStart(2, '0');

    const partitionFolder = path.join(yyyy.toString(), mm, dd, hh); // yyyy/mm/dd/hh partition

    // Update destination folder to include partitioning
    const fullDestFolder = path.join(destFolder, partitionFolder);

    for (const filename of files) {
      const filePath = path.join(sourceFolder, filename);
      const stat = fs.statSync(filePath);

      if (stat.isDirectory()) {
        // Recursively upload files in subdirectories
        await this.uploadAllFilesInFolder(filePath, fullDestFolder);
      } else if (stat.isFile() && filename.endsWith('.json')) {
        // Only upload JSON files
        console.log(`Uploading file: ${filename}`);

        // Construct S3 key using partitioned structure
        const s3Key = path.join(fullDestFolder, filename);
        await this.uploadFile(filePath, s3Key);
      }
    }
  }
}
