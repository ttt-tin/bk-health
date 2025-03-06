import { Module } from "@nestjs/common";
import { ExternalVolumeService } from "./external-volume.service";
import { ExternalVolumeController } from "./external-volume.controller";
import { AthenaModule } from "src/athena/athena..module";
import { S3Module } from "src/s3/s3.module";

@Module({
  imports: [AthenaModule, S3Module],
  controllers: [ExternalVolumeController],
  providers: [ExternalVolumeService],
  exports: [ExternalVolumeService],
})
export class ExternalVolumeModule {}
