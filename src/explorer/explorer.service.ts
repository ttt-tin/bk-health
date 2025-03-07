import { Injectable } from "@nestjs/common";
import { AthenaService } from "src/athena/athena.service";

@Injectable()
export class ExplorerService {
  constructor(private readonly athenaService: AthenaService) {}

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
      let schemaQuery = "";
      let rowCountQuery = "";
      let sizeQuery = "";
      let fileCountQuery = "";

      if (database === "hospital_data") {
        // Queries for structured data
        schemaQuery = `DESCRIBE \`${database}\`.\`${table}\`;`;
        rowCountQuery = `SELECT COUNT(*) AS total_rows FROM \`${database}\`.\`${table}\`;`;
        sizeQuery = `SELECT SUM(total_size) / (1024 * 1024) AS total_size_mb FROM information_schema.table_storage WHERE table_schema = '${database}' AND table_name = '${table}';`;
      } else {
        // Queries for unstructured data (metadata-db)
        fileCountQuery = `SELECT COUNT(*) AS total_files FROM \`${database}\`.\`${table}\`;`;
        schemaQuery = `SELECT file_name, file_size FROM \`${database}\`.\`${table}\` LIMIT 10;`;
      }

      const schemaResults = await this.athenaService.executeQuery(
        schemaQuery,
        database,
        true,
      );

      let schema = [];
      if (database === "hospital_data") {
        schema = schemaResults
          .map((row) => row[0]?.split("\t"))
          .filter(
            (row) =>
              row &&
              row.length >= 2 &&
              !row[0].startsWith("#") &&
              row[0].trim().toLowerCase() !== "col_name",
          )
          .map(([name, type]) => ({ name, type }));
      } else {
        // Unstructured - return file metadata
        schema = schemaResults.map((row) => ({
          name: row[0],
          type: "file",
          size: row[1]
            ? `${(parseInt(row[1]) / (1024 * 1024)).toFixed(2)} MB`
            : "Unknown",
        }));
      }

      // Fetch row count (only for structured data)
      let rowCount = null;
      if (database === "hospital_data") {
        const rowCountResults = await this.athenaService.executeQuery(
          rowCountQuery,
          database,
          true,
        );
        rowCount =
          rowCountResults.length > 0 ? parseInt(rowCountResults[0][0]) : 0;
      }

      // Fetch table size (only for structured data)
      let tableSize = null;
      if (database === "hospital_data") {
        const sizeResults = await this.athenaService.executeQuery(
          sizeQuery,
          database,
          true,
        );
        tableSize =
          sizeResults.length > 0
            ? parseFloat(sizeResults[0][0]).toFixed(2)
            : "Unknown";
      }

      // Fetch file count (only for metadata-db)
      let fileCount = null;
      if (database !== "hospital_data") {
        const fileCountResults = await this.athenaService.executeQuery(
          fileCountQuery,
          database,
          true,
        );
        fileCount =
          fileCountResults.length > 0 ? parseInt(fileCountResults[0][0]) : 0;
      }

      return {
        schema,
        rowCount,
        tableSize: tableSize ? `${tableSize} MB` : null,
        fileCount,
      };
    } catch (error) {
      console.error("Error fetching table info:", error);
      throw new Error("Failed to fetch table info");
    }
  }
}
