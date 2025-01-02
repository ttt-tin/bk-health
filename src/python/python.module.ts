import { Module } from '@nestjs/common';
import { PythonController } from './python.controller';
import { PythonService } from './python.service';
import { HistoryModule } from 'src/history/history.module';

@Module({
  imports: [HistoryModule],
  controllers: [PythonController],
  providers: [PythonService],
})
export class PythonModule {}
