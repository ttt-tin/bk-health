import { Controller, Get, Post, Body, Param, Delete } from "@nestjs/common";
import { DataSourcesService } from "./data-sources.service";
import { CreateDataSourceDto } from "./dtos/create-data-source.dto";
import { DataSource } from "./entities/data-source.entity";

@Controller("data-sources")
export class DataSourcesController {
  constructor(private readonly dataSourcesService: DataSourcesService) {}

  @Post()
  async create(@Body() dto: CreateDataSourceDto): Promise<DataSource> {
    return this.dataSourcesService.create(dto);
  }

  @Get()
  async findAll(): Promise<DataSource[]> {
    return this.dataSourcesService.findAll();
  }

  @Get(":id")
  async findOne(@Param("id") id: string): Promise<DataSource> {
    return this.dataSourcesService.findOne(+id);
  }

  @Delete(":id")
  async remove(@Param("id") id: string): Promise<void> {
    return this.dataSourcesService.remove(+id);
  }
}
