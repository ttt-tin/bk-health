import { Module } from "@nestjs/common";
import { PythonController } from "./python.controller";
import { PythonService } from "./python.service";
import { HistoryModule } from "src/history/history.module";
import { NotificationModule } from "src/notification/notification.module";

@Module({
  imports: [HistoryModule, NotificationModule],
  controllers: [PythonController],
  providers: [PythonService],
})
export class PythonModule {}
