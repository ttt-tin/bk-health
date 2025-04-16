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

  async parseAndSaveFromFile(
    file: Express.Multer.File,
    dbName: string,
  ): Promise<TableColumnEntity[]> {
    const text = file.buffer.toString("utf8");
    const lines = text.split(/\r?\n/).filter(Boolean);

    const columns: TableColumnEntity[] = [];

    for (const line of lines) {
      const [table_name, column_name] = line.split(",").map((s) => s.trim());
      if (!table_name || !column_name) continue;

      const exists = await this.columnRepo.findOne({
        where: {
          schema_name: dbName,
          table_name,
          column_name,
        },
      });

      if (exists) continue;

      const entity = this.columnRepo.create({
        schema_name: dbName,
        table_name,
        column_name,
      });

      columns.push(entity);
    }

    return this.columnRepo.save(columns);
  }

  async findByDatabaseAndTable(
    db: string,
    table: string,
  ): Promise<TableColumnEntity[]> {
    return this.columnRepo.find({
      where: {
        schema_name: db,
        table_name: table,
      },
    });
  }

  async getAllSchemaNames(): Promise<string[]> {
    const schemas = await this.columnRepo
      .createQueryBuilder("column")
      .select("DISTINCT column.schema_name", "schema_name")
      .getRawMany();

    return schemas.map((s) => s.schema_name);
  }
}
