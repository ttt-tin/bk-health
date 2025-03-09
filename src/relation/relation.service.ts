import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { CreateRelationshipDto } from "./dto/relationship.dto";
import { RelationshipEntity } from "./entities/relation.entity";
import { AthenaService } from "src/athena/athena.service";
import { v4 as uuidv4 } from "uuid";

@Injectable()
export class RelationshipService {
  constructor(
    @InjectRepository(RelationshipEntity)
    private relationshipRepo: Repository<RelationshipEntity>,
    private readonly athenaService: AthenaService,
  ) {}

  async create(dtos: CreateRelationshipDto[]) {
    for (let dto of dtos) {
      const id = uuidv4()
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
}
