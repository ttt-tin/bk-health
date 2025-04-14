// table-column.service.ts
import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, Repository } from "typeorm";
import { TableColumnEntity } from "./entities/table-column.entity";
import { DefineTableDto } from "./dto/define-table.dto";

@Injectable()
export class TableColumnService {
  constructor(
    private dataSource: DataSource,

    @InjectRepository(TableColumnEntity)
    private readonly columnRepo: Repository<TableColumnEntity>,
  ) {}

  async saveDefinedTables(
    tables: DefineTableDto[],
  ): Promise<TableColumnEntity[]> {
    const columnsToSave: TableColumnEntity[] = [];

    for (const table of tables) {
      const columns = await this.dataSource.query(
        `
          SELECT column_name
          FROM information_schema.columns
          WHERE table_name = $1 AND table_schema = $2
        `,
        [table.table_name, table.schema_name],
      );

      for (const col of columns) {
        const columnEntity = this.columnRepo.create({
          table_name: table.table_name,
          schema_name: table.schema_name,
          column_name: col.column_name,
        });

        columnsToSave.push(columnEntity);
      }
    }

    return this.columnRepo.save(columnsToSave);
  }
}
