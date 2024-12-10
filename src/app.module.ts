import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ConstraintModule } from './constraint/constraint.module';
import { PythonModule } from './python/python.module';

@Module({
  imports: [ConstraintModule, PythonModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
