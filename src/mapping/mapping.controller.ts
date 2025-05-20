import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Put,
  Delete,
} from "@nestjs/common";
import { MappingService } from "./mapping.service";
import { CreateMappingDto } from "./dto/create-mapping.dto";
import { UpdateMappingDto } from "./dto/update-mapping.dto";
import { AthenaService } from "src/athena/athena.service";
import { v4 as uuidv4 } from "uuid";

@Controller("mappings")
export class MappingController {
  constructor(
    private readonly mappingService: MappingService,
    private readonly athenaService: AthenaService,
  ) {}

  @Post()
  async create(@Body() createMappingDtos: CreateMappingDto[]) {
    try {
      console.log(JSON.stringify(createMappingDtos));
      
      // Tạo một câu query duy nhất cho tất cả các bản ghi
      const values = createMappingDtos.map(dto => {
        const id = uuidv4();
        const now = new Date().toISOString();
        return `(
          '${id}',
          '${dto.dbName}',
          '${dto.dbTable}',
          '${dto.dbColumn}',
          '${dto.standardColumn}',
          '${dto.standardDb}',
          '${dto.standardTable}',
          from_iso8601_timestamp('${now}'),
          from_iso8601_timestamp('${now}')
        )`;
      }).join(',');

      const query = `
        INSERT INTO bk_health_lakehouse_db.mapping (
          id,
          db_name,
          db_table,
          db_column,
          standard_column,
          standard_db,
          standard_table,
          created_at,
          updated_at
        ) VALUES ${values};
      `;

      await this.athenaService.executeQuery(query);

      return {
        success: true,
        message: "Mappings inserted into Athena successfully.",
      };
    } catch (error) {
      console.error(error);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  @Get("all")
  async getAllMappingsFromAthena() {
    try {
      // Construct the Athena query to fetch all mappings
      const query = `
        SELECT * FROM bk_health_lakehouse_db.mapping
      `;

      // Execute the query using AthenaService
      const results = await this.athenaService.executeQuery(query);

      // Check if results exist
      if (!results || results.length === 0) {
        return {
          success: false,
          message: "No data found in Athena.",
        };
      }

      // Return the results to the UI
      return {
        success: true,
        data: results,
      };
    } catch (error) {
      console.error("Error fetching mappings from Athena:", error);
      return {
        success: false,
        message: "Error fetching mappings from Athena.",
      };
    }
  }

  @Get()
  findAll() {
    return this.mappingService.findAll();
  }

  @Get(":dbName")
  findByDBName(@Param("dbName") dbName: string) {
    return this.mappingService.findByDBName(dbName);
  }

  @Get(":dbName/:dbTableName/:standardTabeName")
  findMapping(
    @Param("dbName") dbName: string,
    @Param("dbTableName") dbTableName: string,
    @Param("standardTabeName") standardTabeName: string,
  ) {
    return this.mappingService.findMapping(
      dbName,
      dbTableName,
      standardTabeName,
    );
  }

  @Put(":id")
  update(@Param("id") id: number, @Body() updateMappingDto: UpdateMappingDto) {
    return this.mappingService.update(id, updateMappingDto);
  }

  @Delete(":id")
  remove(@Param("id") id: number) {
    return this.mappingService.remove(id);
  }
}
