import { IsDate, IsString } from 'class-validator';
import { Entity, Column, PrimaryGeneratedColumn, CreateDateColumn, UpdateDateColumn } from 'typeorm';

@Entity('mapping')
export class MappingEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @IsString()
  @Column({ name: 'db_name', type: 'varchar', length: 255, nullable: true })
  dbName: string;

  @IsString()
  @Column({ name: 'db_column', type: 'varchar', length: 255, nullable: true }) 
  dbColumn: string;

  @IsString()
  @Column({ name: 'standard_column', type: 'varchar', length: 255, nullable: true }) 
  standardColumn: string;

  @IsDate()
  @CreateDateColumn({ name: 'created_at', type: 'timestamp', nullable: true })
  createdAt: Date;

  @IsDate()
  @UpdateDateColumn({ name: 'updated_at', type: 'timestamp', nullable: true })
  updatedAt: Date;
}
