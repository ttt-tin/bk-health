// table-column.controller.ts
import { Body, Controller, Post } from "@nestjs/common";
import { TableColumnService } from "./table-column.service";
import { DefineTableDto } from "./dto/define-table.dto";
import { TableColumnEntity } from "./entities/table-column.entity";

@Controller("columns")
export class TableColumnController {
  constructor(private readonly columnService: TableColumnService) {}

  @Post("define")
  async defineTables(
    @Body() body: DefineTableDto[],
  ): Promise<TableColumnEntity[]> {
    return this.columnService.saveDefinedTables(body);
  }
}
