import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { NotificationEntity } from "./entities/notification.entity";

@Injectable()
export class NotificationService {
  constructor(
    @InjectRepository(NotificationEntity)
    private readonly notificationRepository: Repository<NotificationEntity>,
  ) {}

  async gets(query: Record<string, any>): Promise<any> {
    const queryBuilder =
      this.notificationRepository.createQueryBuilder("notification");

    const {
      orderBy = "id",
      orderDirection = "DESC",
      page = 1,
      pageSize = 10,
    } = query;

    queryBuilder.orderBy(
      `notification.${orderBy}`,
      orderDirection.toUpperCase() === "ASC" ? "ASC" : "DESC",
    );

    queryBuilder.skip((page - 1) * pageSize).take(pageSize);

    return await queryBuilder.getMany();
  }

  async test(): Promise<any> {
    try {
      const testData = [
        {
          status: "Success",
          type: "Cleaning",
          desc: "Cleaning run successfully",
          regDate: new Date("2025-04-20T10:00:00Z"),
        },
        {
          status: "Error",
          type: "Cleaning",
          desc: "Cleaning run failed",
          regDate: new Date("2025-04-20T12:00:00Z"),
        },

        {
          status: "Success",
          type: "Cleaning",
          desc: "Cleaning run successfully",
          regDate: new Date("2025-04-21T10:00:00Z"),
        },
        {
          status: "Success",
          type: "Cleaning",
          desc: "Cleaning run successfully",
          regDate: new Date("2025-04-22T10:00:00Z"),
        },
        {
          status: "Success",
          type: "Cleaning",
          desc: "Cleaning run successfully",
          regDate: new Date("2025-04-23T10:00:00Z"),
        },
        {
          status: "Error",
          type: "Cleaning",
          desc: "Cleaning run failed",
          regDate: new Date("2025-04-24T10:00:00Z"),
        },
        {
          status: "Success",
          type: "Cleaning",
          desc: "Cleaning run successfully",
          regDate: new Date("2025-04-30T15:00:00Z"),
        },
        {
          status: "Success",
          type: "Cleaning",
          desc: "Cleaning run successfully",
          regDate: new Date("2025-05-01T08:00:00Z"),
        },
      ];

      for (const data of testData) {
        const notification = this.notificationRepository.create(data);
        await this.notificationRepository.save(notification);
      }
    } catch (error) {
      console.error("Error in test method:", error);
      throw new Error("Test method failed");
    }
  }
}
