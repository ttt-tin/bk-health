import {
  Controller,
  Get,
  Param,
  HttpException,
  HttpStatus,
} from "@nestjs/common";
import { ExplorerService } from "./explorer.service";

@Controller("explorer")
export class ExplorerController {
  constructor(private readonly explorerService: ExplorerService) {}

  // Fetch all tables from the specified database (hospital_data or metadata-db)
  @Get("tables/:database")
  async getTables(@Param("database") database: string): Promise<string[]> {
    try {
      return await this.explorerService.fetchTables(database);
    } catch (error) {
      console.error(`Error fetching tables from ${database}:`, error);
      throw new HttpException(
        "Failed to fetch tables",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  // Fetch table details (columns for structured, file list for unstructured)
  @Get("table-info/:database/:table")
  async getTableInfo(
    @Param("database") database: string,
    @Param("table") table: string,
  ): Promise<string[]> {
    try {
      return await this.explorerService.fetchTableInfo(database, table);
    } catch (error) {
      console.error(
        `Error fetching table info for ${database}.${table}:`,
        error,
      );
      throw new HttpException(
        "Failed to fetch table info",
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
