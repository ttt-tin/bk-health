// table-column.entity.ts
import { Entity, PrimaryGeneratedColumn, Column } from "typeorm";

@Entity("table_columns")
export class TableColumnEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ nullable: true })
  table_name: string;

  @Column({ nullable: true })
  schema_name: string;

  @Column({ nullable: true })
  column_name: string;

  @Column({ nullable: true })
  type: string;
}
