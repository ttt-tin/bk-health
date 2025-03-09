import { Injectable } from "@nestjs/common";
import { S3 } from "aws-sdk";
import * as dayjs from "dayjs"; // Add this import

@Injectable()
export class S3Service {
  private readonly s3: S3;

  constructor() {
    this.s3 = new S3({
      region: process.env.AWS_REGION, // Ensure the region is set
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY,
        secretAccessKey: process.env.AWS_SECRET_KEY,
      },
    });
  }

  private buckets = [
    "bk-health-bucket-landing",
    "bk-health-bucket-raw",
    "bk-health-bucket-trusted",
  ];

  async listFiles(folderPath: string): Promise<any[]> {
    try {
      // Extract folder name from full S3 path (e.g., "s3://landing/unstructure/folder/")
      const folderKey = folderPath.replace(
        `s3://${process.env.AWS_S3_BUCKET_NAME}/`,
        "",
      );

      // List objects in the specified folder
      const response = await this.s3
        .listObjectsV2({
          Bucket: process.env.AWS_S3_BUCKET_NAME,
          Prefix: folderKey, // Fetch all files in this folder
        })
        .promise();

      if (!response.Contents || response.Contents.length === 0) {
        return [];
      }

      // Get metadata for each file
      const fileDetails = await Promise.all(
        response.Contents.map(async (file) => {
          try {
            const head = await this.s3
              .headObject({
                Bucket: process.env.AWS_S3_BUCKET_NAME,
                Key: file.Key!,
              })
              .promise();

            return {
              fileName: file.Key?.split("/").pop() || "Unknown",
              size: head.ContentLength || 0, // File size in bytes
              createdAt: file.LastModified?.toISOString() || "Unknown", // S3 does not store `creation-date`
              lastModified: file.LastModified?.toISOString() || "Unknown",
            };
          } catch (headError) {
            console.error(
              `Error fetching metadata for ${file.Key}:`,
              headError,
            );
            return {
              fileName: file.Key?.split("/").pop() || "Unknown",
              size: 0,
              createdAt: "Unknown",
              lastModified: file.LastModified?.toISOString() || "Unknown",
            };
          }
        }),
      );

      return fileDetails;
    } catch (error) {
      console.error("Error fetching S3 files:", error);
      throw new Error("Failed to fetch S3 file metadata");
    }
  }

  async listALLFiles(bucket) {
    const allFiles = [];
    let continuationToken = undefined;

    do {
      const response = await this.s3
        .listObjectsV2({ Bucket: bucket, ContinuationToken: continuationToken })
        .promise();

      allFiles.push(...(response.Contents || []));
      continuationToken = response.NextContinuationToken;
    } while (continuationToken);

    return allFiles.map((file) => ({
      Key: file.Key,
      LastModified: file.LastModified,
    }));
  }

  async getS3Statistics() {
    const bucketStats = [];
    const fileTypes = {};
    const monthlyUploads = {};
    let totalFiles = 0;

    for (const bucket of this.buckets) {
      const files = await this.listALLFiles(bucket);
      const count = files.length;
      totalFiles += count;
      bucketStats.push({ bucket, count });

      files.forEach((file) => {
        // File type extraction
        const ext = file.Key?.split(".").pop()?.toLowerCase() || "unknown";
        fileTypes[ext] = (fileTypes[ext] || 0) + 1;

        // Monthly uploads calculation
        if (file.LastModified) {
          const month = dayjs(file.LastModified).format("YYYY-MM");
          monthlyUploads[month] = (monthlyUploads[month] || 0) + 1;
        }
      });
    }

    return {
      bucketDistribution: bucketStats.map((b) => ({
        bucket: b.bucket,
        percent:
          totalFiles > 0
            ? Number(((b.count / totalFiles) * 100).toFixed(2))
            : 0,
      })),
      fileTypeStats: Object.entries(fileTypes).map(([type, count]) => ({
        type,
        count,
      })),
      monthlyUploads: Object.entries(monthlyUploads).map(([month, count]) => ({
        month,
        count,
      })),
    };
  }
}
