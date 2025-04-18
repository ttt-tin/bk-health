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
    // Normalize folder name (replace spaces, lowercase, remove invalid chars)
    const folderName =
      dto[0].name
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
    } catch (error) {
      throw new Error(`Failed to create S3 folder: ${error.message}`);
    }

    // Save data source to database
    const dataSource = this.dataSourceRepository.create(dto);
    return this.dataSourceRepository.save(dataSource);
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
    // Fetch data source to get name for S3 folder
    const dataSource = await this.findOne(id);
    const folderName =
      dataSource.name
        .toLowerCase()
        .replace(/[^a-z0-9-_]/g, "-")
        .replace(/-+/g, "-") + "/";

    // Delete all objects in the S3 folder
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

    // Delete data source from database
    const result = await this.dataSourceRepository.delete(id);
    if (result.affected === 0) {
      throw new NotFoundException(`Data Source with ID ${id} not found`);
    }
  }
}
