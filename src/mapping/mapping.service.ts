import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { CreateMappingDto } from "./dto/create-mapping.dto";
import { UpdateMappingDto } from "./dto/update-mapping.dto";
import { MappingEntity } from "./entities/mapping.entity";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import * as parquet from "parquetjs-lite";
import { AthenaService } from "src/athena/athena.service";

/**
 * CREATE TABLE bk_health_lakehouse_db.mapping (
  db_name string,
  db_table string,
  db_column string,
  standard_column string,
  standard_table string,
  standard_db string,
  created_at timestamp,
  updated_at timestamp)
LOCATION 's3://bk-health-bucket-trusted'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd',
  'format'='parquet'
);
 */

@Injectable()
export class MappingService {
  private readonly bucketName = "bk-health-bucket-trusted";
  private readonly s3Prefix = "mapping/";

  constructor(
    @InjectRepository(MappingEntity)
    private readonly mappingRepository: Repository<MappingEntity>,

    private readonly athenaService: AthenaService,
  ) {}

  async create(
    createMappingDtos: CreateMappingDto[],
  ): Promise<MappingEntity[]> {
    const results = [];
    for (const createMappingDto of createMappingDtos) {
      const mapping = this.mappingRepository.create(createMappingDto);
      const result = await this.mappingRepository.save(mapping);
      results.push(result);
    }
    await this.uploadMappingToS3();
    return results;
  }

  async findAll(): Promise<MappingEntity[]> {
    const results = await this.mappingRepository.find();
    await this.uploadMappingToS3();
    return results;
  }

  async uploadMappingToS3(): Promise<any> {
    const mappings = await this.mappingRepository.find();

    if (!mappings.length) return;

    const values = mappings
      .map((item) => {
        const format = (v: any) =>
          v ? `'${v.toString().replace(/'/g, "''")}'` : "NULL";

        const timestamp = (v: Date | undefined) =>
          v
            ? `TIMESTAMP '${v.toISOString().replace("T", " ").replace("Z", "")}'`
            : "NULL";

        const dbName = format(item.dbName);
        const dbTable = format(item.dbTable);
        const dbColumn = format(item.dbColumn);
        const standardColumn = format(item.standardColumn);
        const standardTable = format(item.standardTable);
        const standardDb = format(item.standardDb);
        const createdAt = timestamp(item.createdAt);
        const updatedAt = timestamp(item.updatedAt);

        return `
      SELECT ${dbName} AS db_name, ${dbTable} AS db_table, ${dbColumn} AS db_column, 
             ${standardColumn} AS standard_column, ${standardTable} AS standard_table, 
             ${standardDb} AS standard_db, ${createdAt} AS created_at, ${updatedAt} AS updated_at
      WHERE NOT EXISTS (
        SELECT 1 FROM bk_health_lakehouse_db.mapping 
        WHERE db_name = ${dbName} AND db_table = ${dbTable} AND db_column = ${dbColumn}
          AND standard_column = ${standardColumn} AND standard_table = ${standardTable}
          AND standard_db = ${standardDb}
      )`;
      })
      .join("\nUNION ALL\n");

    const query = `
    INSERT INTO bk_health_lakehouse_db.mapping (
      db_name, db_table, db_column, standard_column, standard_table, standard_db, created_at, updated_at
    )
    ${values};
  `;

    try {
      await this.athenaService.executeQuery(query, "bk_health_lakehouse_db");
      console.log("✅ Inserted mapping data into Athena table.");
    } catch (error) {
      console.error("❌ Failed to upload mapping to Athena:", error);
      throw error;
    }
  }

  async findByDBName(dbName: string): Promise<MappingEntity[]> {
    return this.mappingRepository.find({ where: { dbName } });
  }

  async update(
    id: number,
    updateMappingDto: UpdateMappingDto,
  ): Promise<MappingEntity> {
    await this.mappingRepository.update(id, updateMappingDto);
    await this.uploadMappingToS3();
    return this.mappingRepository.findOne({ where: { id } });
  }

  async findMapping(
    dbName: string,
    dbTableName: string,
    standardTabeName: string,
  ): Promise<MappingEntity[]> {
    return this.mappingRepository.find({
      where: {
        dbName,
        dbTable: dbTableName,
        standardTable: standardTabeName,
      },
    });
  }

  async remove(id: number): Promise<void> {
    await this.mappingRepository.delete(id);
    await this.uploadMappingToS3();
  }
}
