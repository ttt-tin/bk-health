import { Injectable } from "@nestjs/common";
import {
  AthenaClient,
  ListDataCatalogsCommand,
  ListDatabasesCommand,
  ListTableMetadataCommand,
  StartQueryExecutionCommand,
  GetTableMetadataCommand,
} from "@aws-sdk/client-athena";
import { v4 as uuidv4 } from "uuid";

@Injectable()
export class AthenaService {
  private readonly DATABASE_NAME = "hospital_data";
  private readonly athenaClient: AthenaClient;

  constructor() {
    this.athenaClient = new AthenaClient({
      region: process.env.AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY,
        secretAccessKey: process.env.AWS_SECRET_KEY,
      },
    });
  }

  async fetchCatalogs(): Promise<string[]> {
    try {
      const command = new ListDataCatalogsCommand({});
      const response = await this.athenaClient.send(command);
      return (
        response.DataCatalogsSummary?.map((catalog) => catalog.CatalogName!) ||
        []
      );
    } catch (err) {
      console.error("Error fetching catalogs:", err);
      throw new Error(err.message || "Failed to fetch catalogs.");
    }
  }

  async fetchDatabases(selectedCatalog: string): Promise<string[]> {
    try {
      const command = new ListDatabasesCommand({
        CatalogName: selectedCatalog,
      });
      const response = await this.athenaClient.send(command);
      return response.DatabaseList?.map((db) => db.Name!) || [];
    } catch (err) {
      console.error("Error fetching databases:", err);
      throw new Error(err.message || "Failed to fetch databases.");
    }
  }

  async fetchTables(
    selectedCatalog: string,
    selectedDatabase: string,
  ): Promise<string[]> {
    try {
      const command = new ListTableMetadataCommand({
        CatalogName: selectedCatalog,
        DatabaseName: selectedDatabase,
      });
      const response = await this.athenaClient.send(command);
      return response.TableMetadataList?.map((table) => table.Name!) || [];
    } catch (err) {
      console.error("Error fetching tables:", err);
      throw new Error(err.message || "Failed to fetch tables.");
    }
  }

  async fetchSchema(
    selectedCatalog: string,
    selectedDatabase: string,
    selectedTable: string,
  ): Promise<any[]> {
    try {
      const command = new GetTableMetadataCommand({
        CatalogName: selectedCatalog,
        DatabaseName: selectedDatabase,
        TableName: selectedTable,
      });
      const response = await this.athenaClient.send(command);
      return response.TableMetadata?.Columns || [];
    } catch (err) {
      console.error("Error fetching schema:", err);
      throw new Error(err.message || "Failed to fetch schema.");
    }
  }

  async executeQuery(query: string): Promise<string> {
    try {
      const command = new StartQueryExecutionCommand({
        QueryString: query,
        QueryExecutionContext: { Database: "hospital_data" },
        ResultConfiguration: {
          OutputLocation: `s3://${process.env.AWS_ATHENA_OUTPUT_BUCKET}/athena-results/`,
        },
      });

      const response = await this.athenaClient.send(command);
      return response.QueryExecutionId!; // Trả về QueryExecutionId để theo dõi
    } catch (err) {
      console.error("Error executing Athena query:", err);
      throw new Error(err.message || "Failed to execute query.");
    }
  }

  async insertTableMetadata(
    tableName: string,
    columnName: string,
    primaryKey: string,
  ): Promise<void> {
    const generatedId = uuidv4();

    const query = `
      INSERT INTO ${this.DATABASE_NAME}.tables (id, table_name, column_name, primary_key)
      VALUES ('${generatedId}', '${tableName}', '${columnName}', '${primaryKey}');
    `;
    await this.executeQuery(query);
  }

  async updateTableMetadata(
    id: string,
    tableName: string,
    columnName: string,
    primaryKey: string,
  ): Promise<void> {
    const query = `
      UPDATE tables
      SET table_name = '${tableName}', column_name = '${columnName}', primary_key = '${primaryKey}'
      WHERE id = '${id}';
    `;
    await this.executeQuery(query);
  }
}
