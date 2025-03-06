import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { CreateRelationshipDto } from "./dto/relationship.dto";
import { RelationshipEntity } from "./entities/relation.entity";

@Injectable()
export class RelationshipService {
  constructor(
    @InjectRepository(RelationshipEntity)
    private relationshipRepo: Repository<RelationshipEntity>,
  ) {}

  async create(dto: CreateRelationshipDto) {
    return this.relationshipRepo.save(dto);
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
