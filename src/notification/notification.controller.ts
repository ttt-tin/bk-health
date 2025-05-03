import { Controller, Get, Query } from "@nestjs/common";
import { NotificationService } from "./notification.service";

@Controller("notification")
export class NotificationController {
  constructor(private readonly notificationService: NotificationService) {}

  @Get()
  async gets(@Query() query: Record<string, any>): Promise<any> {
    return this.notificationService.gets(query);
  }

  @Get("test")
  async test(): Promise<any> {
    return this.notificationService.test();
  }
}
