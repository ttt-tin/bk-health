import {
  Controller,
  Get,
  Query,
  HttpException,
  HttpStatus,
  Body,
  Post,
  BadRequestException,
} from "@nestjs/common";
import { v4 as uuidv4 } from "uuid";
import { AthenaService } from "./athena.service";

@Controller("athena")
export class AthenaController {
  constructor(private readonly athenaService: AthenaService) {}

  @Post("universal-keys")
  async addUniversalKeys(
    @Body("database") database: string,
    @Body("table_name") table_name: string,
    @Body("universal_keys") universal_keys: string[][],
  ) {
    try {
      // Loop through each universal key and insert it as a concatenated string
      for (const fields of universal_keys) {
        const concatenatedFields = fields.join(","); // Concatenate fields with commas
        const id = uuidv4(); // Generate a new unique ID for each universal key

        // Construct the query to insert the universal key (concatenated string)
        const query = `
          INSERT INTO ${process.env.BK_HEALTH_LAKEHOUSE_DB}.tables (id, table_name, column_name)
          VALUES ('${id}', '${table_name}', '${concatenatedFields}');
        `;

        // Execute the query
        await this.athenaService.executeQuery(query);
      }

      return { message: "Universal keys added successfully!" };
    } catch (error) {
      console.error("Error inserting universal keys:", error);
      return {
        message: "Failed to insert universal keys",
        error: error.message,
      };
    }
  }

  @Post("create")
  async createTableMetadata(
    @Body("id") id: string,
    @Body("table_name") tableName: string,
    @Body("column_name") columnName: string,
    @Body("database") database: string,
  ) {
    await this.athenaService.updateTableMetadata(
      id,
      tableName,
      columnName,
      database,
    );
    return { message: "Data updated successfully" };
  }

  @Get("universal-keys")
  async getUniversalKeys(
    @Query("table_name") tableName: string,
    @Query("database") database: string,
  ) {
    try {
      const universalKeys = await this.athenaService.getUniversalKeys(
        tableName,
        database,
      );
      return { universal_keys: universalKeys };
    } catch (error) {
      console.error("Error fetching universal keys:", error);
      throw new Error("Failed to fetch universal keys");
    }
  }

  @Get("metadata")
  async getTableMetadata(@Query("table_name") tableName: string) {
    if (!tableName) {
      throw new BadRequestException("Table name is required.");
    }

    const query = `
    SELECT *
    FROM tables
    WHERE table_name = '${tableName}';
  `;
    const result = await this.athenaService.executeQuery(query);
    return result[0];
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

  @Get("check-empty")
  async isHospitalDataEmpty(): Promise<boolean> {
    const tables = await this.athenaService.listTables(
      "AwsDataCatalog",
      "hospital_data",
    );
    return tables.length === 0;
  }
}
