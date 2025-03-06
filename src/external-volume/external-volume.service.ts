import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
} from "@nestjs/common";
import { AthenaService } from "src/athena/athena.service";
import {
  BlobServiceClient,
  ContainerClient,
  StorageSharedKeyCredential,
} from "@azure/storage-blob";

@Injectable()
export class ExternalVolumeService {
  constructor(
    private readonly athenaService: AthenaService,
    private readonly s3Client: S3Client,
  ) {} // Assuming AthenaService contains executeQuery

  async createExternalVolume(
    volumeName: string,
    containerName: string,
    connectionString: string,
  ) {
    try {
      const bucketName = process.env.AWS_S3_BUCKET_NAME;
      if (!bucketName) {
        throw new Error(
          "S3 bucket name is not defined in environment variables.",
        );
      }

      const newVolumeName = `${volumeName + "_external"}/`; // Folder key (with trailing slash)

      const command = new PutObjectCommand({
        Bucket: bucketName,
        Key: `unstructure/${newVolumeName}/`,
      });

      await this.s3Client.send(command);

      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS external_volume_data (
          volume_name STRING,
          container_name STRING,
          connection_string STRING
        )
        LOCATION 's3://bk-health-metadata/'
        TBLPROPERTIES (
            'table_type'='ICEBERG',
            'format'='parquet'
        );
      `;

      await this.athenaService.executeQuery(
        createTableQuery,
        process.env.LAKEHOUSE_DB,
      );

      // Step 3: Insert the new record into external_volume_data
      const insertQuery = `
        INSERT INTO external_volume_data (volume_name, container_name, connection_string)
        VALUES ('${volumeName + "_external_volume_table"}', '${containerName}', '${connectionString}');
      `;

      await this.athenaService.executeQuery(
        insertQuery,
        process.env.LAKEHOUSE_DB,
      );

      return {
        message: `Volume '${volumeName}' created successfully in S3 and recorded in Iceberg.`,
      };
    } catch (error) {
      console.error("Error creating external volume in S3:", error);
      throw new InternalServerErrorException(
        "Failed to create external volume.",
      );
    }
  }

  async getBlobStorageClient(volumeName: string): Promise<{
    container_name: string;
    containerClient: ContainerClient;
  }> {
    // Step 1: Fetch storage connection details from external_volume_data
    const query = `
    SELECT container_name, connection_string
    FROM external_volume_data
    WHERE volume_name = '${volumeName}'
    LIMIT 1;
  `;

    const results = await this.athenaService.executeQuery(
      query,
      process.env.LAKEHOUSE_DB,
    );

    if (!results.length) {
      throw new BadRequestException(
        "Storage connection not found for the given volume.",
      );
    }

    const { container_name, connection_string } = results[0];

    const properties = connection_string.split(";").reduce(
      (acc, item) => {
        const [key, value] = item.split("=");
        if (key && value) {
          acc[key.trim().toLowerCase()] = value.trim();
        }
        return acc;
      },
      {} as Record<string, string>,
    );

    // Step 2: Connect to Azure Blob Storage
    const blobServiceClient = new BlobServiceClient(
      `https://${properties["accountname"]}.blob.${properties["endpointsuffix"]}`,
      new StorageSharedKeyCredential(
        properties["accountname"],
        properties["accountkey"],
      ),
    );

    const containerClient =
      blobServiceClient.getContainerClient(container_name);

    return { container_name, containerClient };
  }

  async getStorageFiles(volumeName: string): Promise<any[]> {
    try {
      const { container_name, containerClient } =
        await this.getBlobStorageClient(volumeName);
      const fileList: any[] = [];

      // Step 3: List files from the container
      for await (const blob of containerClient.listBlobsFlat()) {
        fileList.push({
          file_name: blob.name,
          file_path: `${containerClient.url}/${blob.name}`,
          bucket_name: container_name,
          file_size: blob.properties.contentLength || 0,
          upload_timestamp: blob.properties.lastModified,
        });
      }

      return fileList;
    } catch (error) {
      console.error("Error fetching storage files:", error);
      throw new BadRequestException("Failed to fetch storage files.");
    }
  }

  async uploadFile(volumeName: string, file: Express.Multer.File) {
    try {
      const { container_name, containerClient } =
        await this.getBlobStorageClient(volumeName);
      const blockBlobClient = containerClient.getBlockBlobClient(
        file.originalname,
      );

      await blockBlobClient.uploadData(file.buffer, {
        blobHTTPHeaders: { blobContentType: file.mimetype },
      });

      return {
        message: `Upload file to external volume ${volumeName} successfully.`,
      };
    } catch (error) {
      throw new InternalServerErrorException(
        "Error uploading file to Azure Blob Storage",
      );
    }
  }
}
