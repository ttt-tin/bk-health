import { Controller, Post, UseInterceptors, UploadedFile, Body } from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { ConstraintService } from './constraint.service';

@Controller('constraint')
export class ConstraintController {
  constructor(private readonly constraintService: ConstraintService) {}

  @Post('generate')
  @UseInterceptors(FileInterceptor('file'))
  async generateConstraintFile(
    @Body('tableName') tableName: string,
    @Body('constraintCondition') constraintCondition: string,
  ) {
    return this.constraintService.createConstraintFile(tableName, constraintCondition);
  }
}
