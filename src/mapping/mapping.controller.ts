import { Controller, Get, Post, Body, Param, Put, Delete } from '@nestjs/common';
import { MappingService } from './mapping.service';
import { CreateMappingDto } from './dto/create-mapping.dto';
import { UpdateMappingDto } from './dto/update-mapping.dto';

@Controller('mappings')
export class MappingController {
  constructor(private readonly mappingService: MappingService) {}

  @Post()
  create(@Body() createMappingDtos: CreateMappingDto[]) {
    return this.mappingService.create(createMappingDtos);
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
