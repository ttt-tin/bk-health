import { Injectable, NotFoundException } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { DataSource } from "./entities/data-source.entity";
import { CreateDataSourceDto } from "./dtos/create-data-source.dto";
import {
  S3Client,
  PutObjectCommand,
  DeleteObjectsCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";

@Injectable()
export class DataSourcesService {
  constructor(
    @InjectRepository(DataSource)
    private readonly dataSourceRepository: Repository<DataSource>,
    private readonly s3Client: S3Client,
  ) {}

  async create(dto: CreateDataSourceDto): Promise<DataSource> {
    if (!dto || !dto.name) {
      throw new Error('Name is required');
    }

    // Normalize folder name (replace spaces, lowercase, remove invalid chars)
    const folderName =
      dto.name
        .toLowerCase()
        .replace(/[^a-z0-9-_]/g, "-")
        .replace(/-+/g, "-") + "/";

    // Create S3 folder (empty object with trailing slash)
    try {
      const bucketName = process.env.AWS_S3_BUCKET_NAME;
      await this.s3Client.send(
        new PutObjectCommand({
          Bucket: bucketName,
          Key: `structure/${folderName}`,
          Body: "",
        }),
      );

      // Tạo data source với path là đường dẫn S3
      const dataSource = this.dataSourceRepository.create({
        name: dto.name,
        dataType: dto.dataType || null,
        fileType: dto.fileType || null,
        path: `s3://${bucketName}/structure/${folderName}`,
      });

      return this.dataSourceRepository.save(dataSource);
    } catch (error) {
      throw new Error(`Failed to create S3 folder: ${error.message}`);
    }
  }

  async findAll(): Promise<DataSource[]> {
    return this.dataSourceRepository.find();
  }

  async findOne(id: number): Promise<DataSource> {
    const dataSource = await this.dataSourceRepository.findOne({
      where: { id },
    });
    if (!dataSource) {
      throw new NotFoundException(`Data Source with ID ${id} not found`);
    }
    return dataSource;
  }

  async remove(id: number): Promise<void> {
    const dataSource = await this.findOne(id);
    if (dataSource) {
      // Xóa bucket trên S3
      const folderName =
        dataSource.name
          .toLowerCase()
          .replace(/[^a-z0-9-_]/g, "-")
          .replace(/-+/g, "-") + "/";

      try {
        const bucketName = process.env.AWS_S3_BUCKET_NAME;
        // List objects in the folder
        const listResponse = await this.s3Client.send(
          new ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: `structure/${folderName}`,
          }),
        );

        if (listResponse.Contents?.length) {
          // Delete all objects
          await this.s3Client.send(
            new DeleteObjectsCommand({
              Bucket: bucketName,
              Delete: {
                Objects: listResponse.Contents.map((item) => ({ Key: item.Key })),
              },
            }),
          );
        }
      } catch (error) {
        throw new Error(`Failed to delete S3 folder: ${error.message}`);
      }

      // Xóa data source từ database
      await this.dataSourceRepository.remove(dataSource);
    }
  }
}
