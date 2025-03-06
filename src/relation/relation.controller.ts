import { Controller, Post, Get, Delete, Param, Body } from "@nestjs/common";
import { RelationshipService } from "./relationship.service";
import { CreateRelationshipDto } from "./relationship.dto";

@Controller("relationships")
export class RelationshipController {
  constructor(private readonly relationshipService: RelationshipService) {}

  @Post()
  create(@Body() dto: CreateRelationshipDto) {
    return this.relationshipService.create(dto);
  }

  @Get()
  findAll() {
    return this.relationshipService.findAll();
  }

  @Get(":id")
  findOne(@Param("id") id: number) {
    return this.relationshipService.findOne(id);
  }

  @Delete(":id")
  delete(@Param("id") id: number) {
    return this.relationshipService.delete(id);
  }
}
