import { Controller, Post, Body, Get, Param, UseInterceptors, UploadedFile } from "@nestjs/common";
import { ExternalVolumeService } from "./external-volume.service";
import { FileInterceptor } from "@nestjs/platform-express";

@Controller("external-volume")
export class ExternalVolumeController {
  constructor(private readonly externalVolumeService: ExternalVolumeService) {}

  @Post("create")
  async createExternalVolume(
    @Body()
    body: {
      volumeName: string;
      containerName: string;
      connectionString: string;
    },
  ) {
    return this.externalVolumeService.createExternalVolume(
      body.volumeName,
      body.containerName,
      body.connectionString,
    );
  }

  @Get("files/:volumeName")
  async getStorageFiles(@Param("volumeName") volumeName: string) {
    return this.externalVolumeService.getStorageFiles(volumeName);
  }

  @Post("upload/:volumeName")
  @UseInterceptors(FileInterceptor("file"))
  async uploadFile(
    @Param("volumeName") volumeName: string,
    @UploadedFile() file: Express.Multer.File,
  ) {
    return this.externalVolumeService.uploadFile(volumeName, file);
  }
}
