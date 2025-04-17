// table-column.controller.ts
import {
  Body,
  Controller,
  Get,
  Param,
  Post,
  Query,
  UploadedFile,
  UseInterceptors,
} from "@nestjs/common";
import { TableColumnService } from "./table-column.service";
import { DefineTableDto } from "./dto/define-table.dto";
import { TableColumnEntity } from "./entities/table-column.entity";
import { FileInterceptor } from "@nestjs/platform-express";

@Controller("columns")
export class TableColumnController {
  constructor(private readonly columnService: TableColumnService) {}

  @Post("define")
  async defineTables(
    @Body() body: DefineTableDto[],
  ): Promise<TableColumnEntity[]> {
    return this.columnService.saveDefinedTables(body);
  }

  @Post("upload")
  @UseInterceptors(FileInterceptor("file"))
  async uploadDefineFile(
    @UploadedFile() file: Express.Multer.File,
    @Query("databaseName") databaseName: string,
  ): Promise<TableColumnEntity[]> {
    return this.columnService.parseAndSaveFromFile(file, databaseName);
  }

  // GET theo database và table
  @Get()
  async getByDatabaseAndTable(
    @Query("databaseName") dbName: string,
    @Query("tableName") tableName: string,
  ): Promise<TableColumnEntity[]> {
    return this.columnService.findByDatabaseAndTable(dbName, tableName);
  }

  @Get("schemas")
  async getAllSchemaNames(): Promise<string[]> {
    return this.columnService.getAllSchemaNames();
  }

  @Get("columns/:schemaName")
  async getAllColumnNames(@Param('schemaName') schemaName: string): Promise<string[]> {
    return this.columnService.getAllColumnNames(schemaName);
  }
}
