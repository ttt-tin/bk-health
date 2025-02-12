// history.module.ts
import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { HistoryService } from "./history.service";
import { HistoryEntity } from "../python/entities/history-run.entity";
import { HistoryController } from "./history.controller";

@Module({
  imports: [TypeOrmModule.forFeature([HistoryEntity])],
  controllers: [HistoryController],
  providers: [HistoryService],
  exports: [HistoryService, TypeOrmModule],
})
export class HistoryModule {}
