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

  async uploadFile(filePath: string, s3Key: string, bucket: string = null): Promise<void> {
    const fileStream = fs.createReadStream(filePath);
    console.log(filePath)

    const params = {
      Bucket: bucket ?? process.env.AWS_S3_BUCKET_NAME,
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

  async uploadAllFilesInFolder(
    sourceFolder: string,
    destFolder: string,
    baseDestFolder?: string,
    relativePath: string = ""
  ): Promise<void> {
    const files = fs.readdirSync(sourceFolder);
  
    // Chỉ tính toán phân vùng nếu không có baseDestFolder (chỉ xảy ra ở lần gọi đầu tiên)
    if (!baseDestFolder) {
      const currentDate = new Date();
      const yyyy = currentDate.getFullYear();
      const mm = (currentDate.getMonth() + 1).toString().padStart(2, '0');
      const dd = currentDate.getDate().toString().padStart(2, '0');
      const hh = currentDate.getHours().toString().padStart(2, '0');
  
      // Tạo đường dẫn phân vùng
      const partitionFolder = `${yyyy}/${mm}/${dd}/${hh}`;
      baseDestFolder = `${destFolder}/${partitionFolder}`;
    }
  
    for (const filename of files) {
      const filePath = path.join(sourceFolder, filename);
      const stat = fs.statSync(filePath);
  
      if (stat.isDirectory()) {
        // Gọi đệ quy với relativePath được nối thêm cấu trúc thư mục con
        await this.uploadAllFilesInFolder(
          filePath,
          destFolder,
          baseDestFolder,
          path.join(relativePath, filename)
        );
      } else if (stat.isFile() && filename.endsWith('.json')) {
        console.log(`Uploading file: ${filename}`);
  
        // Tạo S3 key với baseDestFolder cố định và thêm relativePath
        const s3Key = path.join(baseDestFolder, relativePath, filename);
        await this.uploadFile(filePath, s3Key);
      }
    }
  }
  

  async uploadAllFilesCSVInFolder(sourceFolder: string, destFolder: string, bucket: string): Promise<void> {
    const files = fs.readdirSync(sourceFolder);

    // Get the current date and time for partitioning
    const currentDate = new Date();
    const yyyy = currentDate.getFullYear();
    const mm = (currentDate.getMonth() + 1).toString().padStart(2, '0');
    const dd = currentDate.getDate().toString().padStart(2, '0');
    const hh = currentDate.getHours().toString().padStart(2, '0');

    const partitionFolder = path.join(yyyy.toString(), mm, dd, hh); // yyyy/mm/dd/hh partition

    // Update destination folder to include partitioning
    let fullDestFolder = path.join(destFolder, partitionFolder);  // Sử dụng fullDestFolder cho cấu trúc phân cấp

    for (const filename of files) {
      const filePath = path.join(sourceFolder, filename);
      const stat = fs.statSync(filePath);

      if (stat.isDirectory()) {
        // Recursively upload files in subdirectories
        await this.uploadAllFilesCSVInFolder(filePath, fullDestFolder, bucket);
      } else if (stat.isFile() && filename.endsWith('.csv') && !filename.endsWith('_merged.csv')) {
        // Only upload CSV files
        console.log(`Uploading file: ${filename}`);

        // Construct S3 key using partitioned structure
        const s3Key = path.join(partitionFolder, filename); // Chỉ sử dụng partitionFolder cho S3 key

        await this.uploadFile(filePath, s3Key, bucket);
      }
    }
  }

}
