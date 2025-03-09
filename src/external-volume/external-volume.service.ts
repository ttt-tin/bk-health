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
  ) {}

  // Helper function to format Date to Athena-compatible TIMESTAMP string
  private formatTimestamp(date: Date): string {
    const pad = (num: number) => num.toString().padStart(2, "0");
    const year = date.getUTCFullYear();
    const month = pad(date.getUTCMonth() + 1); // Months are 0-based
    const day = pad(date.getUTCDate());
    const hours = pad(date.getUTCHours());
    const minutes = pad(date.getUTCMinutes());
    const seconds = pad(date.getUTCSeconds());
    const milliseconds = date.getUTCMilliseconds().toString().padStart(3, "0");
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
  }

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

      const newVolumeName = `${volumeName}_external/`; // Folder key (with trailing slash)

      // Step 1: Create the folder in S3 (assumed to trigger table creation elsewhere)
      const command = new PutObjectCommand({
        Bucket: bucketName,
        Key: `unstructure/${newVolumeName}`,
      });
      await this.s3Client.send(command);

      // Step 2: Create the external_volume_data table (unchanged)
      const createVolumeTableQuery = `
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
        createVolumeTableQuery,
        process.env.LAKEHOUSE_DB,
      );

      // Step 3: Insert the new record into external_volume_data (unchanged)
      const insertVolumeQuery = `
        INSERT INTO external_volume_data (volume_name, container_name, connection_string)
        VALUES ('${volumeName}_external_volume_table', '${containerName}', '${connectionString}');
      `;
      await this.athenaService.executeQuery(
        insertVolumeQuery,
        process.env.LAKEHOUSE_DB,
      );

      // Step 4: Fetch file metadata from Azure using getBlobStorageClient
      const { containerClient } = await this.getBlobStorageClient(
        `${volumeName}_external_volume_table`,
      );
      const fileList: any[] = [];
      for await (const blob of containerClient.listBlobsFlat()) {
        fileList.push({
          file_name: blob.name,
          file_path: `${containerClient.url}/${blob.name}`,
          bucket_name: containerName,
          file_size: blob.properties.contentLength || 0,
          upload_timestamp: blob.properties.lastModified,
        });
      }

      // Step 5: Insert file metadata into the pre-existing volume-specific table
      if (fileList.length > 0) {
        const values = fileList
          .map(
            (file) => `
          ('${file.file_name}',
           '${file.file_path}',
           '${file.bucket_name}',
           ${file.file_size},
           TIMESTAMP '${this.formatTimestamp(file.upload_timestamp)}')`,
          )
          .join(", ");

        const insertFilesQuery = `
          INSERT INTO ${volumeName}_external_volume_table (file_name, file_path, bucket_name, file_size, upload_timestamp)
          VALUES ${values};
        `;
        await this.athenaService.executeQuery(insertFilesQuery, "metadata-db");
      }

      return {
        message: `Volume '${volumeName}' created successfully in S3 and file metadata recorded in Iceberg.`,
      };
    } catch (error) {
      console.error(
        "Error creating external volume or saving file metadata:",
        error,
      );
      throw new InternalServerErrorException(
        "Failed to create external volume or save file metadata.",
      );
    }
  }

  async getBlobStorageClient(volumeName: string): Promise<{
    container_name: string;
    containerClient: ContainerClient;
  }> {
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

      // Step 1: Upload the file to Azure Blob Storage
      await blockBlobClient.uploadData(file.buffer, {
        blobHTTPHeaders: { blobContentType: file.mimetype },
      });

      // Step 2: Get metadata for the uploaded file
      const fileSize = file.buffer.length; // Size from the buffer
      const uploadTimestamp = new Date(); // Current timestamp as upload time
      const filePath = `${containerClient.url}/${file.originalname}`;

      // Step 3: Insert metadata into the table
      const values = `
        ('${file.originalname}',
         '${filePath}',
         '${container_name}',
         ${fileSize},
         TIMESTAMP '${this.formatTimestamp(uploadTimestamp)}')
      `;
      const insertFilesQuery = `
        INSERT INTO ${volumeName} (file_name, file_path, bucket_name, file_size, upload_timestamp)
        VALUES ${values};
      `;
      await this.athenaService.executeQuery(insertFilesQuery, "metadata-db");

      return {
        message: `Upload file to external volume ${volumeName} successfully and metadata recorded.`,
      };
    } catch (error) {
      console.error("Error uploading file or saving metadata:", error);
      throw new InternalServerErrorException(
        "Error uploading file to Azure Blob Storage or saving metadata",
      );
    }
  }
}
