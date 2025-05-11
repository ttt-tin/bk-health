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

@Entity("data_source")
export class DataSource {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ name: "name", type: "varchar", length: 255, nullable: false })
  name: string;

  @Column({ name: "data_type", type: "enum", enum: DataType, nullable: true })
  dataType: DataType;

  @Column({ name: "file_type", type: "enum", enum: FileType, nullable: true })
  fileType: FileType;

  @Column({ name: "path", type: "varchar", length: 255, nullable: false })
  path: string;
}
