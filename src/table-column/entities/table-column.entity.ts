// table-column.entity.ts
import { Entity, PrimaryGeneratedColumn, Column } from 'typeorm';

@Entity('table_columns')
export class TableColumnEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  table_name: string;

  @Column()
  schema_name: string;

  @Column()
  column_name: string;

  @Column()
  type: string;
}
