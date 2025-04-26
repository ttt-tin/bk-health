import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { MappingService } from './mapping.service';
import { MappingController } from './mapping.controller';
import { MappingEntity } from './entities/mapping.entity';
import { AthenaModule } from 'src/athena/athena..module';

@Module({
  imports: [TypeOrmModule.forFeature([MappingEntity]), AthenaModule],
  providers: [MappingService],
  controllers: [MappingController],
  exports: [MappingService]
})
export class MappingModule {}
