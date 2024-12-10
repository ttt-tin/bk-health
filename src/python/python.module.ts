import { Module } from '@nestjs/common';
import { PythonController } from './python.controller';
import { PythonService } from './python.service';

@Module({
  controllers: [PythonController],
  providers: [PythonService]
})
export class PythonModule {}
