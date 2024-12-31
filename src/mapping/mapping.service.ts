import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { CreateMappingDto } from './dto/create-mapping.dto';
import { UpdateMappingDto } from './dto/update-mapping.dto';
import { MappingEntity } from './entities/mapping.entity';
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
            const result = await this.mappingRepository.save(mapping);
            results.push(result);
        });

        return results;
    }

    async findAll(): Promise<MappingEntity[]> {
        return this.mappingRepository.find();
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
