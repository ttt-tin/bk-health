import { Module } from '@nestjs/common';
import { ConstraintController } from './constraint.controller';
import { ConstraintService } from './constraint.service';

@Module({
  controllers: [ConstraintController],
  providers: [ConstraintService],
})
export class ConstraintModule {}
