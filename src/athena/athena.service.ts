import { Injectable } from "@nestjs/common";
import {
  AthenaClient,
  ListDataCatalogsCommand,
  ListDatabasesCommand,
  ListTableMetadataCommand,
  StartQueryExecutionCommand,
  GetTableMetadataCommand,
  GetQueryResultsCommand,
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

  async updateTableMetadata(
    id: string,
    tableName: string,
    columnName: string,
  ): Promise<void> {
    if (!id) {
      const generatedId = uuidv4();
      const query = `
        INSERT INTO ${this.DATABASE_NAME}.tables (id, table_name, column_name)
        VALUES ('${generatedId}', '${tableName}', '${columnName}');
      `;
      await this.executeQuery(query);
    } else {
      const query = `
        UPDATE tables
        SET table_name = '${tableName}', column_name = '${columnName}'
        WHERE id = '${id}';
      `;
      await this.executeQuery(query);
    }
  }

  async fetchData(
    catalog: string,
    database: string,
    table: string,
  ): Promise<any> {
    const command = new StartQueryExecutionCommand({
      QueryString: `SELECT * FROM ${table} LIMIT 10`,
      QueryExecutionContext: {
        Database: database,
      },
      ResultConfiguration: {
        OutputLocation: `s3://${process.env.AWS_ATHENA_OUTPUT_BUCKET}`,
      },
    });

    const executionResponse = await this.athenaClient.send(command);
    const queryExecutionId = executionResponse.QueryExecutionId;

    await new Promise((resolve) => setTimeout(resolve, 5000));

    const getResultsCommand = new GetQueryResultsCommand({
      QueryExecutionId: queryExecutionId,
    });
    const results = await this.athenaClient.send(getResultsCommand);

    return this.transformAthenaResults(results.ResultSet?.Rows || []);
  }

  transformAthenaResults(rawResults: any[]): any[] {
    if (!rawResults || rawResults.length === 0) {
      return [];
    }

    // Giả sử hàng đầu tiên là header
    var headerRow = rawResults[0].Data;
    var headers = headerRow.map(function (item: any) {
      return item.VarCharValue;
    });

    // Xử lý các hàng còn lại
    var rows = [];
    for (var i = 1; i < rawResults.length; i++) {
      var rowData = rawResults[i].Data;
      var rowObj: any = {};
      for (var j = 0; j < headers.length; j++) {
        // Nếu không có dữ liệu cho cột nào đó, gán null
        rowObj[headers[j]] =
          rowData[j] && rowData[j].VarCharValue !== undefined
            ? rowData[j].VarCharValue
            : null;
      }
      rows.push(rowObj);
    }
    return rows;
  }
}
