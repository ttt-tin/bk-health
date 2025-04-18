import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { DataSourcesService } from "./data-sources.service";
import { DataSourcesController } from "./data-sources.controller";
import { DataSource } from "./entities/data-source.entity";
import { S3Service } from "src/s3/s3.service";
import { S3Module } from "src/s3/s3.module";

@Module({
  imports: [TypeOrmModule.forFeature([DataSource]), S3Module],
  controllers: [DataSourcesController],
  providers: [DataSourcesService, S3Service],
})
export class DataSourcesModule {}
