import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RelationshipEntity } from "./entities/relation.entity";
import { RelationshipController } from "./relation.controller";
import { RelationshipService } from "./relation.service";

@Module({
  imports: [TypeOrmModule.forFeature([RelationshipEntity])],
  controllers: [RelationshipController],
  providers: [RelationshipService],
})
export class RelationshipModule {}
