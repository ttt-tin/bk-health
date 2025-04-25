import { Entity, PrimaryGeneratedColumn, Column } from "typeorm";

export enum DataType {
  BATCH = "batch",
  STREAMING = "streaming",
}

export enum FileType {
  CSV = "csv",
  JSON = "json",
  PARQUET = "parquet",
}

@Entity()
export class DataSource {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  name: string;

  @Column({ type: "enum", enum: DataType, nullable: true })
  dataType: DataType;

  @Column({ type: "enum", enum: FileType, nullable: true })
  fileType: FileType;
}
