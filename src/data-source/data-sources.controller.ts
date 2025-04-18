import {
  Controller,
  Get,
  Post,
  Delete,
  Body,
  Param,
  ParseIntPipe,
  HttpCode,
} from "@nestjs/common";
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
  async findOne(@Param("id", ParseIntPipe) id: number): Promise<DataSource> {
    return this.dataSourcesService.findOne(id);
  }

  @Delete(":id")
  @HttpCode(204)
  async remove(@Param("id", ParseIntPipe) id: number): Promise<void> {
    await this.dataSourcesService.remove(id);
  }
}
