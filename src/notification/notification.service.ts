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
}
