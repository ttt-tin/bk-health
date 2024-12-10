import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ConstraintModule } from './constraint/constraint.module';
import { PythonModule } from './python/python.module';
import { UploadModule } from './upload/upload.module';

@Module({
  imports: [ConstraintModule, PythonModule, UploadModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
