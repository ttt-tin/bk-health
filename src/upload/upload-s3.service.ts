import { Injectable } from '@nestjs/common';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import * as fs from 'fs';
import * as path from 'path';
import * as mime from 'mime-types';

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


  async uploadFileForTextract(
    file: Express.Multer.File, // File from request
    bucketName: string,
    folderPath: string,
  ) {
    // Lấy tên file và phần mở rộng
    const fileName = file.originalname;  // Directly use the original name
    const fileExtension = path.extname(fileName).toLowerCase();

    // Kiểm tra nếu file có phải là PDF hoặc hình ảnh (JPEG, PNG, TIFF)
    if (!['.pdf', '.jpg', '.jpeg', '.png', '.tiff'].includes(fileExtension)) {
      throw new Error('Only PDF and image files (JPG, PNG, TIFF) are supported.');
    }

    try {
      // Ensure folderPath uses forward slashes (for S3 compatibility)
      const s3FolderPath = folderPath.replace(/\\/g, '/');  // Replace backslashes with forward slashes
      const fileKey = `${s3FolderPath}/${fileName}`;  // Correct S3 folder path format

      // Get MIME type using mime-types (fallback to 'application/octet-stream' if undefined)
      const mimeType = mime.lookup(fileName) || 'application/octet-stream';

      // Upload lên S3 using file.buffer (since it's directly in memory)
      const uploadParams = {
        Bucket: bucketName,
        Key: fileKey,  // Correct S3 folder/file path
        Body: file.buffer,  // Use the buffer directly
        ContentType: mimeType,  // Use MIME type from mime.lookup() or fallback type
      };

      const command = new PutObjectCommand(uploadParams);
      await this.s3.send(command);
      console.log(`Uploaded: ${fileName}`);
      return fileKey; // Return the file key to trigger Textract
    } catch (error) {
      console.error(`Error uploading file ${fileName}:`, error);
      throw new Error(`Error uploading file ${fileName}: ${error.message}`);
    }
  }


}
