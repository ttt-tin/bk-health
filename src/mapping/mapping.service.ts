import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { CreateMappingDto } from './dto/create-mapping.dto';
import { UpdateMappingDto } from './dto/update-mapping.dto';
import { MappingEntity } from './entities/mapping.entity';
import * as fs from 'fs';

@Injectable()
export class MappingService {
    constructor(
        @InjectRepository(MappingEntity)
        private readonly mappingRepository: Repository<MappingEntity>,
    ) { }

    async create(createMappingDtos: CreateMappingDto[]): Promise<MappingEntity[]> {
        const results = []
        createMappingDtos.forEach(async createMappingDto => {
            const mapping = this.mappingRepository.create(createMappingDto);
            console.log(mapping);
            const result = await this.mappingRepository.save(mapping);

            await this.findAll();
            results.push(result);
        });

        return results;
    }

    async findAll(): Promise<any> {
        const results = await this.mappingRepository.find();

        const output = results.reduce((acc: any, item: any) => {
            if (!acc.tables) {
                acc.tables = [];
            }
        
            const table = acc.tables.find(t => t.source_table === item.dbTable);
            
            if (!table) {
                acc.tables.push({
                    source_table: item.dbTable,
                    mapping: {}
                });
            }
        
            const targetTable = acc.tables.find(t => t.source_table === item.dbTable);
            targetTable.mapping[item.standardColumn] = item.dbColumn;
        
            if (!acc.database) {
                acc.database = item.dbName;
            }
        
            return acc;
        }, {});

        const jsonString = JSON.stringify(output, null, 2);

        fs.writeFile('mapping.json', jsonString, 'utf8', (err) => {
            if (err) {
              console.error('Error writing file', err);
            } else {
              console.log('File has been saved!');
            }
          });

        return results;
    }

    async findByDBName(dbName: string): Promise<MappingEntity[]> {
        return this.mappingRepository.find({ where: { dbName } });
    }

    async update(id: number, updateMappingDto: UpdateMappingDto): Promise<MappingEntity> {
        await this.mappingRepository.update(id, updateMappingDto);
        return this.mappingRepository.findOne({ where: { id } });
    }

    async remove(id: number): Promise<void> {
        await this.mappingRepository.delete(id);
    }
}
