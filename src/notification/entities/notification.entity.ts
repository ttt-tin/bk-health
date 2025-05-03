import { IsDate, IsString } from "class-validator";
import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
} from "typeorm";

@Entity("notification")
export class NotificationEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @IsString()
  @Column({ name: "status", type: "varchar", length: 255, nullable: true })
  status: string;

  @IsString()
  @Column({ name: "type", type: "varchar", length: 255, nullable: true })
  type: string;

  @IsString()
  @Column({ name: "desc", type: "varchar", length: 255, nullable: true })
  desc: string;

  @IsDate()
  @CreateDateColumn({ name: "reg_date", type: "timestamp", nullable: true })
  regDate: Date;
}
