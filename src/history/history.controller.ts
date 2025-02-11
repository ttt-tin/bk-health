import { Controller, Get, Query } from "@nestjs/common";
import { HistoryService } from "./history.service";

@Controller("history")
export class HistoryController {
  constructor(private readonly historyService: HistoryService) {}

  @Get()
  async getHistory(@Query("limit") limit?: number): Promise<any> {
    return this.historyService.getHistory(limit);
  }
}
