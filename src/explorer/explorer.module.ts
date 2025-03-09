import { Module } from "@nestjs/common";
import { ExplorerService } from "./explorer.service";
import { ExplorerController } from "./explorer.controller";
import { AthenaService } from "src/athena/athena.service";
import { S3Service } from "src/s3/s3.service";

@Module({
  imports: [],
  controllers: [ExplorerController],
  providers: [ExplorerService, AthenaService, S3Service],
  exports: [ExplorerService], // In case other modules need it
})
export class ExplorerModule {}
