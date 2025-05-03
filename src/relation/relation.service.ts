import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { CreateRelationshipDto } from "./dto/relationship.dto";
import { RelationshipEntity } from "./entities/relation.entity";
import { AthenaService } from "src/athena/athena.service";
import { v4 as uuidv4 } from "uuid";
import { TableColumnEntity } from "src/table-column/entities/table-column.entity";

@Injectable()
export class RelationshipService {
  private BK_HEALTH_LAKEHOUSE_DB = 'bk_health_lakehouse_db'
 
  constructor(
    @InjectRepository(RelationshipEntity)
    private relationshipRepo: Repository<RelationshipEntity>,

    @InjectRepository(TableColumnEntity)
    private columnRepo: Repository<TableColumnEntity>,

    private readonly athenaService: AthenaService,
  ) {}

  async create(dtos: CreateRelationshipDto[]) {
    for (const dto of dtos) {
      const id = uuidv4();
      const query = `INSERT INTO ${this.BK_HEALTH_LAKEHOUSE_DB}.relationships (id, table_reference, table_was_reference, pri_key, fo_key)
        VALUES ('${id}', '${dto.tableReference}', '${dto.tableWasReference}', '${dto.priKey}', '${dto.foKey}');`;
      await this.athenaService.executeQuery(query);
    }
    return true;
  }

  async findAll() {
    return this.relationshipRepo.find();
  }

  async findOne(id: number) {
    return this.relationshipRepo.findOne({ where: { id } });
  }

  async delete(id: number) {
    return this.relationshipRepo.delete(id);
  }

  async autoDetectReferences() {
    const databaseName = "hospital_data";

    // Step 1: Fetch all tables
    const showTablesQuery = `SHOW TABLES IN ${databaseName}`;
    const tablesResult = await this.athenaService.executeQuery(showTablesQuery);

    const tableNames = tablesResult
      .map((row) => {
        const firstKey = Object.keys(row)[0];
        return row[firstKey]?.toLowerCase();
      })
      .filter(Boolean);

    if (tableNames.length === 0) {
      throw new Error(`No tables found in Athena database ${databaseName}`);
    }

    const referenceSuffixes = [
      "_reference",
      "reference",
      "_id",
      "id",
      "Id",
      "ID",
    ];

    const allColumns: { tableName: string; columnName: string }[] = [];

    // Step 2: Fetch columns for each table
    for (const table of tableNames) {
      const showColumnsQuery = `SHOW COLUMNS IN ${databaseName}.${table}`;
      const columnsResult =
        await this.athenaService.executeQuery(showColumnsQuery);

      for (const row of columnsResult) {
        const columnNameKey = Object.keys(row)[0];
        allColumns.push({
          tableName: table,
          columnName: row[columnNameKey],
        });
      }
    }

    const tableSet = new Set(tableNames);

    const potentialRelations: Partial<RelationshipEntity>[] = [];

    // Step 3: Detect relations
    for (const col of allColumns) {
      const colName = col.columnName;
      const colTable = col.tableName.toLowerCase();

      for (const suffix of referenceSuffixes) {
        if (colName.toLowerCase().endsWith(suffix.toLowerCase())) {
          const baseName = colName
            .slice(0, colName.length - suffix.length)
            .toLowerCase();

          if (baseName && tableSet.has(baseName) && baseName !== colTable) {
            potentialRelations.push({
              tableReference: col.tableName,
              tableWasReference: baseName,
              priKey: "id",
              foKey: col.columnName,
            });
            break;
          }
        }
      }
    }

    // Step 4: Save detected relations
    if (potentialRelations.length) {
      await this.relationshipRepo.save(potentialRelations);
    }

    return { inserted: potentialRelations.length };
  }
}
