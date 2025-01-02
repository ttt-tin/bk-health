import { Injectable } from '@nestjs/common';
import {
  AthenaClient,
  ListDataCatalogsCommand,
  ListDatabasesCommand,
  ListTableMetadataCommand,
  GetTableMetadataCommand,
} from '@aws-sdk/client-athena';

@Injectable()
export class AthenaService {
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
      console.error('Error fetching catalogs:', err);
      throw new Error(err.message || 'Failed to fetch catalogs.');
    }
  }

  async fetchDatabases(selectedCatalog: string): Promise<string[]> {
    try {
      const command = new ListDatabasesCommand({ CatalogName: selectedCatalog });
      const response = await this.athenaClient.send(command);
      return response.DatabaseList?.map((db) => db.Name!) || [];
    } catch (err) {
      console.error('Error fetching databases:', err);
      throw new Error(err.message || 'Failed to fetch databases.');
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
      console.error('Error fetching tables:', err);
      throw new Error(err.message || 'Failed to fetch tables.');
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
      console.error('Error fetching schema:', err);
      throw new Error(err.message || 'Failed to fetch schema.');
    }
  }
}
