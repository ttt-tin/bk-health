import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RelationshipEntity } from "./entities/relation.entity";
import { RelationshipController } from "./relation.controller";
import { RelationshipService } from "./relation.service";
import { AthenaModule } from "src/athena/athena..module";
import { TableColumnEntity } from "src/table-column/entities/table-column.entity";

@Module({
  imports: [
    TypeOrmModule.forFeature([RelationshipEntity, TableColumnEntity]),
    AthenaModule,
  ],
  controllers: [RelationshipController],
  providers: [RelationshipService],
})
export class RelationshipModule {}
