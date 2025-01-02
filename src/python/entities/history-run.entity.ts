import { IsDate, IsNumber, IsString } from 'class-validator';
import { Entity, Column, PrimaryGeneratedColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('history')
export class HistoryEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @IsString()
  @Column({ name: 'status', type: 'varchar', length: 255, nullable: true })
  status: string;

  @IsDate()
  @CreateDateColumn({ name: 'start_time', type: 'timestamp', nullable: true }) 
  startTime: Date;

  @IsNumber()
  @Column({ name: 'duration', type: 'int', nullable: true }) 
  duration: number;
}
