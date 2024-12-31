import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { MappingService } from './mapping.service';
import { MappingController } from './mapping.controller';
import { MappingEntity } from './entities/mapping.entity';

@Module({
  imports: [TypeOrmModule.forFeature([MappingEntity])],
  providers: [MappingService],
  controllers: [MappingController],
  exports: [MappingService]
})
export class MappingModule {}
