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
  ) {}

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

  @Get("file")
  async runUploadFile(
    @Query("folderPath") folderPath: string,
    @Query("databaseName") databaseName: string,
  ): Promise<any> {
    try {
      await this.uploadService.uploadAllFilesInFolder(
        folderPath,
        process.env.DEST_FOLDER,
        databaseName,
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
    @Body()
    requestData: { patient_id?: string; volume: string; [key: string]: any },
  ): Promise<any> {
    try {
      if (!file) throw new Error("File upload failed, no file received.");
      if (!requestData || !requestData.volume)
        throw new Error("Volume is required to determine the upload folder.");

      const bucketName = process.env.AWS_S3_BUCKET_NAME;
      const folderPath = `unstructure/${requestData.volume.replace(/_volume_table$/, "")}`;
      const fileExt = file.originalname.split(".").pop();
      const newFileName = `${Date.now()}-${uuidv4()}.${fileExt}`;

      const fileKey = await this.uploadService.uploadFileForTextract(
        { ...file, originalname: newFileName },
        bucketName,
        folderPath,
      );

      const filePath = `s3://${bucketName}/${fileKey}`;
      const createDate = new Date().toISOString();

      let patientId = requestData.patient_id;

      // If patient_id is missing, find it from other attributes
      if (!patientId) {
        const possibleKeys = Object.keys(requestData).filter(
          (key) => key !== "volume" && key !== "patient_id" && requestData[key],
        );

        if (possibleKeys.length === 0) {
          throw new Error("No valid attributes found to determine patient_id.");
        }

        const whereCondition = possibleKeys
          .map((key) => `${key} = '${requestData[key]}'`)
          .join(" OR ");
        const findPatientQuery = `SELECT id FROM patient_repaired WHERE ${whereCondition} LIMIT 1;`;

        const result = await this.athenaService.executeQuery(findPatientQuery);
        if (result.length === 0) {
          throw new Error(
            "No matching patient_id found for provided attributes.",
          );
        }

        patientId = result[0].id;
      }

      const tableName = requestData.volume;

      // Insert into the dynamically named table
      const insertQuery = `
      INSERT INTO ${tableName} (patient_id, file_name, file_path, bucket_name, file_size, upload_timestamp)
      VALUES ('${patientId}', '${newFileName}', '${filePath}', '${bucketName}', ${file.size}, current_timestamp);
    `;

      await this.athenaService.executeQuery(insertQuery, "metadata-db");

      return {
        message: "File uploaded and information saved successfully",
        filePath,
        patientId,
        table: tableName,
      };
    } catch (error) {
      return { message: error.message };
    }
  }
}
