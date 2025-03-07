import { Module } from "@nestjs/common";
import { ExplorerService } from "./explorer.service";
import { ExplorerController } from "./explorer.controller";
import { AthenaService } from "src/athena/athena.service";

@Module({
  imports: [],
  controllers: [ExplorerController],
  providers: [ExplorerService, AthenaService],
  exports: [ExplorerService], // In case other modules need it
})
export class ExplorerModule {}
