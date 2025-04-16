import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { AthenaModule } from "src/athena/athena..module";
import { TableColumnController } from "./table-column.controller";
import { TableColumnService } from "./table-column.service";
import { TableColumnEntity } from "./entities/table-column.entity";

@Module({
  imports: [TypeOrmModule.forFeature([TableColumnEntity]), AthenaModule],
  controllers: [TableColumnController],
  providers: [TableColumnService],
})
export class TableColumnModule {}
