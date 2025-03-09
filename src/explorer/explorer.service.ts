import { Injectable } from "@nestjs/common";
import { AthenaService } from "src/athena/athena.service";
import { S3Service } from "src/s3/s3.service";

@Injectable()
export class ExplorerService {
  constructor(
    private readonly athenaService: AthenaService,
    private readonly s3Service: S3Service,
  ) {}

  // Fetch tables from Athena for a given database
  async fetchTables(database: string): Promise<string[]> {
    const query = `SHOW TABLES IN \`${database}\``;
    const results = await this.athenaService.executeQuery(
      query,
      "hospital_data",
      true,
    );

    // Athena response format: [{ tab_name: "table1" }, { tab_name: "table2" }]
    return results.map((row) => row[0]);
  }

  // Fetch table details (columns for hospital_data, file names for metadata-db)
  async fetchTableInfo(database: string, table: string): Promise<any> {
    try {
      if (database === "hospital_data") {
        // Structured data: Fetch schema via Athena DESCRIBE query

        const query = `DESCRIBE \`${database}\`.\`${table}\`;`;

        const results = await this.athenaService.executeQuery(
          query,
          database,
          true,
        );

        return results
          .map((row) => row[0]?.split("\t"))
          .filter(
            (row) =>
              row &&
              row.length >= 2 &&
              !row[0].startsWith("#") &&
              row[0].trim().toLowerCase() !== "col_name",
          )
          .map(([name, type]) => ({ name, type }));
      } // Unstructured data: Query S3 bucket (landing/unstructure/{folderName})

      const folderName = table.replace("_volume_table", ""); // Remove "_volume_table" postfix
      const bucketName = process.env.AWS_S3_BUCKET_NAME;
      const awsRegion = process.env.AWS_REGION; // Get AWS region from environment
      const s3Path = `s3://${bucketName}/unstructure/${folderName}/`; // Fetch file metadata from S3
      const s3Files = await this.s3Service.listFiles(s3Path);

      if (!s3Files || s3Files.length === 0) {
        throw new Error(`No files found in S3 path: ${s3Path}`);
      } // Calculate total size and find metadata

      const totalSizeMb =
        s3Files.reduce((sum, file) => sum + file.size, 0) / (1024 * 1024);
      const fileCount = s3Files.length; // Determine internal/external type based on folder name
      const type = folderName.includes("external") ? "external" : "internal"; // Extract creation and last modified timestamps
      const lastUpdated = new Date(
        Math.max(
          ...s3Files.map((file) => new Date(file.lastModified).getTime()),
        ),
      ).toISOString(); // Construct S3 folder URI
      const folderUri = `https://${bucketName}.s3.${awsRegion}.amazonaws.com/unstructure/${folderName}/`;

      return {
        folderName,
        totalSizeMb: totalSizeMb.toFixed(2) + " MB",
        fileCount,
        type,
        lastUpdated,
        awsRegion, // ✅ Added AWS region
        folderUri, // ✅ Added folder URI
      };
    } catch (error) {
      console.error("Error fetching table info:", error);

      throw new Error("Failed to fetch table info");
    }
  }
}
