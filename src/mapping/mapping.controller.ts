import { Controller, Get, Post, Body, Param, Put, Delete } from '@nestjs/common';
import { MappingService } from './mapping.service';
import { CreateMappingDto } from './dto/create-mapping.dto';
import { UpdateMappingDto } from './dto/update-mapping.dto';

@Controller('mappings')
export class MappingController {
  constructor(private readonly mappingService: MappingService) {}

  @Post()
  async create(@Body() createMappingDtos: CreateMappingDto[]) {
    try {
      console.log(createMappingDtos)
      const results = await this.mappingService.create(createMappingDtos);
      return {
        data: results,
        success: true
      }
    }
    catch (error) {
      return {
        message: error.message,
        success: false
      }
    }
  }

  @Get()
  findAll() {
    return this.mappingService.findAll();
  }

  @Get(':dbName')
  findByDBName(@Param('dbName') dbName: string) {
    return this.mappingService.findByDBName(dbName);
  }

  @Put(':id')
  update(@Param('id') id: number, @Body() updateMappingDto: UpdateMappingDto) {
    return this.mappingService.update(id, updateMappingDto);
  }

  @Delete(':id')
  remove(@Param('id') id: number) {
    return this.mappingService.remove(id);
  }
}
