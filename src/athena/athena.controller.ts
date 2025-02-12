import {
  Controller,
  Get,
  Query,
  HttpException,
  HttpStatus,
  Body,
  Post,
  Put,
} from "@nestjs/common";
import { AthenaService } from "./athena.service";

@Controller("athena")
export class AthenaController {
  constructor(private readonly athenaService: AthenaService) { }

  @Post("create")
  async createTableMetadata(
    @Body("id") id: string,
    @Body("table_name") tableName: string,
    @Body("column_name") columnName: string,
    @Body("primary_key") primaryKey: string,
  ) {
    await this.athenaService.updateTableMetadata(
      id,
      tableName,
      columnName,
      primaryKey,
    );
    return { message: "Data updated successfully" };
  }

  @Get("catalogs")
  async getCatalogs() {
    try {
      return await this.athenaService.fetchCatalogs();
    } catch (err) {
      throw new HttpException(err.message, HttpStatus.BAD_REQUEST);
    }
  }

  @Get("databases")
  async getDatabases(@Query("catalog") catalog: string) {
    if (!catalog) {
      throw new HttpException("Catalog is required", HttpStatus.BAD_REQUEST);
    }
    try {
      return await this.athenaService.fetchDatabases(catalog);
    } catch (err) {
      throw new HttpException(err.message, HttpStatus.BAD_REQUEST);
    }
  }

  @Get("tables")
  async getTables(
    @Query("catalog") catalog: string,
    @Query("database") database: string,
  ) {
    if (!catalog || !database) {
      throw new HttpException(
        "Catalog and Database are required",
        HttpStatus.BAD_REQUEST,
      );
    }
    try {
      return await this.athenaService.fetchTables(catalog, database);
    } catch (err) {
      throw new HttpException(err.message, HttpStatus.BAD_REQUEST);
    }
  }

  @Get("schema")
  async getSchema(
    @Query("catalog") catalog: string,
    @Query("database") database: string,
    @Query("table") table: string,
  ) {
    if (!catalog || !database || !table) {
      throw new HttpException(
        "Catalog, Database, and Table are required",
        HttpStatus.BAD_REQUEST,
      );
    }
    try {
      return await this.athenaService.fetchSchema(catalog, database, table);
    } catch (err) {
      throw new HttpException(err.message, HttpStatus.BAD_REQUEST);
    }
  }

  @Get("data")
  async getData(
    @Query("catalog") catalog: string,
    @Query("database") database: string,
    @Query("table") table: string,
  ) {
    if (!catalog || !database || !table) {
      throw new HttpException(
        "Catalog, Database, and Table are required",
        HttpStatus.BAD_REQUEST,
      );
    }
    try {
      return await this.athenaService.fetchData(catalog, database, table);
    } catch (err) {
      throw new HttpException(err.message, HttpStatus.BAD_REQUEST);
    }
  }
}
