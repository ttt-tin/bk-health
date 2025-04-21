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
      const query = `INSERT INTO hospital_data.relationships (id, table_reference, table_was_reference, pri_key, fo_key)
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
    const allColumns = await this.columnRepo.find();
    const tableNames = new Set(
      allColumns.map((col) => col.table_name.toLowerCase()),
    );

    const referenceSuffixes = [
      "_reference",
      "reference",
      "_id",
      "id",
      "Id",
      "ID",
    ];
    const potentialRelations: RelationshipEntity[] = [];

    for (const col of allColumns) {
      const colName = col.column_name;
      const colTable = col.table_name.toLowerCase();

      for (const suffix of referenceSuffixes) {
        if (colName.toLowerCase().endsWith(suffix.toLowerCase())) {
          const baseName = colName
            .slice(0, colName.length - suffix.length)
            .toLowerCase();

          if (baseName && tableNames.has(baseName) && baseName !== colTable) {
            potentialRelations.push({
              tableReference: col.table_name,
              tableWasReference: baseName,
              priKey: "id",
              foKey: col.column_name,
            } as RelationshipEntity);
            break; // avoid duplicate detection from multiple suffixes
          }
        }
      }
    }

    if (potentialRelations.length) {
      await this.relationshipRepo.save(potentialRelations);
    }

    return { inserted: potentialRelations.length };
  }
}
