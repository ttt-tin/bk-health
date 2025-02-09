import {
  Body,
  Controller,
  Get,
  Post,
  Query,
  UseInterceptors,
  UploadedFile,
} from "@nestjs/common";
import { Express } from "express";
import { UploadS3Service } from "./upload-s3.service";
import { FileInterceptor } from "@nestjs/platform-express";
import { AthenaService } from "src/athena/athena.service";
import { v4 as uuidv4 } from "uuid";

@Controller("upload")
export class UploadController {
  constructor(
    private readonly uploadService: UploadS3Service,
    private readonly athenaService: AthenaService,
  ) { }

  @Get()
  async runUpload(
    @Query("bucketName") bucketName: string,
    @Query("folderPath") folderPath: string,
  ): Promise<any> {
    try {
      await this.uploadService.uploadAllFilesCSVInFolder(
        folderPath,
        "./",
        bucketName,
      );
      return true;
    } catch (error) {
      return error.message;
    }
  }

  @Post("file")
  @UseInterceptors(FileInterceptor("file"))
  async uploadFileForTextract(
    @UploadedFile() file: Express.Multer.File,
  ): Promise<any> {
    try {
      const bucketName = process.env.AWS_S3_BUCKET_NAME;
      const folderPath = "unstructure"; // Define folder path for S3
      if (!file) {
        throw new Error("File upload failed, no file received.");
      }

      const fileKey = await this.uploadService.uploadFileForTextract(
        file,
        bucketName,
        folderPath,
      );
      return { message: "File uploaded successfully", fileKey };
    } catch (error) {
      return { message: error.message };
    }
  }

  @Post("upload-file")
  @UseInterceptors(FileInterceptor("file"))
  async uploadFile(
    @UploadedFile() file: Express.Multer.File,
    @Body("user_id") userId: string,
  ): Promise<any> {
    try {
      if (!file) throw new Error("File upload failed, no file received.");
      if (!userId) throw new Error("User ID is required.");

      const bucketName = process.env.AWS_S3_BUCKET_NAME;

      console.log("bucketName", bucketName);
      const folderPath = "unstructure"; // Định nghĩa folder trên S3

      // ✅ 1. Upload file lên S3
      const fileExt = file.originalname.split(".").pop(); // Lấy phần mở rộng
      const newFileName = `${Date.now()}-${uuidv4()}.${fileExt}`; // Tạo tên mới

      console.log("New file name:", newFileName);

      const fileKey = await this.uploadService.uploadFileForTextract(
        { ...file, originalname: newFileName }, // Gửi file với tên mới
        bucketName,
        folderPath,
      );
      
      const filePath = `s3://${bucketName}/${fileKey}`;
      const fileId = fileKey.split("/").pop(); // Lấy tên file làm ID (có thể thay đổi)

      // ✅ 2. Insert vào bảng `user_files`
      const createDate = new Date().toISOString();
      const insertUserFileQuery = `
        INSERT INTO user_files (user_id, file_id, create_date)
        VALUES ('${userId}', '${fileId}', '${createDate}');
      `;
      await this.athenaService.executeQuery(insertUserFileQuery);

      // ✅ 3. Insert vào bảng `medical_analysis`
      const insertMedicalAnalysisQuery = `
        INSERT INTO user_files (user_id, file_id, create_date)
        VALUES (
          '${userId}', 
          '${filePath}', 
          '${createDate}'
        );
      `;
      await this.athenaService.executeQuery(insertMedicalAnalysisQuery);

      return {
        message: "File uploaded and information saved successfully",
        filePath,
      };
    } catch (error) {
      return { message: error.message };
    }
  }
}
