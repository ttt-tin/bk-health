// table-column.service.ts
import { HttpException, HttpStatus, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, Repository } from "typeorm";
import { TableColumnEntity } from "./entities/table-column.entity";
import { DefineTableDto } from "./dto/define-table.dto";
import {
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";
import { Readable, Transform } from "stream";
import { parse } from "csv-parse";
import { StartQueryExecutionCommand } from "@aws-sdk/client-athena";
import { AthenaService } from "src/athena/athena.service";

interface SchemaField {
  name: string;
  type: string;
}
@Injectable()
export class TableColumnService {
  private readonly logger = new Logger(TableColumnService.name);
  constructor(
    private dataSource: DataSource,
    @InjectRepository(TableColumnEntity)
    private readonly columnRepo: Repository<TableColumnEntity>,
    private readonly s3Client: S3Client,
    private readonly athenaService: AthenaService,
  ) {}

  async saveDefinedTables(
    tables: DefineTableDto[],
  ): Promise<TableColumnEntity[]> {
    const columnsToSave: TableColumnEntity[] = [];

    for (const table of tables) {
      const columns = await this.dataSource.query(
        `
          SELECT column_name
          FROM information_schema.columns
          WHERE table_name = $1 AND table_schema = $2
        `,
        [table.table_name, table.schema_name],
      );

      for (const col of columns) {
        const columnEntity = this.columnRepo.create({
          table_name: table.table_name,
          schema_name: table.schema_name,
          column_name: col.column_name,
        });

        columnsToSave.push(columnEntity);
      }
    }

    return this.columnRepo.save(columnsToSave);
  }

  async parseAndSaveFromFile(
    file: Express.Multer.File,
    dbName: string,
  ): Promise<TableColumnEntity[]> {
    const text = file.buffer.toString("utf8");
    const lines = text.split(/\r?\n/).filter(Boolean);

    const columns: TableColumnEntity[] = [];

    for (const line of lines) {
      const [table_name, column_name] = line.split(",").map((s) => s.trim());
      if (!table_name || !column_name) continue;

      const exists = await this.columnRepo.findOne({
        where: {
          schema_name: dbName,
          table_name,
          column_name,
        },
      });

      if (exists) continue;

      const entity = this.columnRepo.create({
        schema_name: dbName,
        table_name,
        column_name,
      });

      columns.push(entity);
    }

    return this.columnRepo.save(columns);
  }

  async findByDatabaseAndTable(
    db: string,
    table: string,
  ): Promise<TableColumnEntity[]> {
    return this.columnRepo.find({
      where: {
        schema_name: db,
        table_name: table,
      },
    });
  }

  async getAllSchemaNames(): Promise<string[]> {
    const schemas = await this.columnRepo
      .createQueryBuilder("column")
      .select("DISTINCT column.schema_name", "schema_name")
      .getRawMany();

    return schemas.map((s) => s.schema_name);
  }

  async getAllColumnNames(schemaName: string): Promise<string[]> {
    const schemas = await this.columnRepo
      .createQueryBuilder("column")
      .select("DISTINCT column.table_name", "table_name")
      .andWhere("column.schema_name = :schemaName", { schemaName })
      .andWhere("column.table_name IS NOT NULL")
      .getRawMany();

    return schemas.map((s) => s.table_name);
  }

  async getSchemaColumns(schemaName: string): Promise<
    {
      tableName: string;
      columns: { name: string; type: string }[];
    }[]
  > {
    const rows = await this.columnRepo.find({
      where: { schema_name: schemaName },
    });

    const tablesMap = new Map<string, { name: string; type: string }[]>();

    for (const row of rows) {
      if (!tablesMap.has(row.table_name)) {
        tablesMap.set(row.table_name, []);
      }
      tablesMap.get(row.table_name).push({
        name: row.column_name,
        type: row.type,
      });
    }

    return Array.from(tablesMap.entries()).map(([tableName, columns]) => ({
      tableName,
      columns,
    }));
  }

  async detectSchemas(
    bucket: string,
    prefix = "structure/",
    sampleLines = 10,
  ): Promise<string> {
    try {
      const listObjectsCommand = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
      });

      const { Contents } = await this.s3Client.send(listObjectsCommand);
      if (!Contents || Contents.length === 0) {
        this.logger.log(
          `No objects found in bucket "${bucket}" with prefix "${prefix}".`,
        );
        return "No objects found.";
      }

      const csvFiles = Contents.filter((obj) => obj.Key?.endsWith(".csv"));

      // Group files by folder (schema name)
      const filesBySchema: Record<string, string[]> = {};
      for (const obj of csvFiles) {
        const key = obj.Key!;
        const parts = key.replace(prefix, "").split("/");

        if (parts.length < 2) continue; // Ignore files not in a subfolder
        const schemaName = parts[0];

        if (!filesBySchema[schemaName]) {
          filesBySchema[schemaName] = [];
        }
        filesBySchema[schemaName].push(key);
      }

      let successCount = 0;
      let failCount = 0;

      // Loop over each folder (schema)
      for (const [schemaName, keys] of Object.entries(filesBySchema)) {
        for (const key of keys) {
          const fileName = key.split("/").pop() ?? key;
          const tableName = fileName.split("_")[0];

          const schemaExists = await this.columnRepo.findOne({
            where: { table_name: tableName, schema_name: schemaName },
          });

          if (schemaExists) {
            this.logger.log(
              `Schema already exists for table "${tableName}" in schema "${schemaName}". Skipping ${key}`,
            );
            continue;
          }

          try {
            const schema = await this.detectCsvSchema(bucket, key, sampleLines);
            await this.insertSchemaIntoDatabaseBulk(
              tableName,
              schemaName,
              schema,
            );
            this.logger.log(
              `Successfully inserted schema for: ${tableName} (schema: ${schemaName})`,
            );
            successCount++;
          } catch (error) {
            this.logger.error(`Failed to process ${key}: ${error.message}`);
            failCount++;
          }
        }
      }

      return `Schema detection complete. Success: ${successCount}, Failed: ${failCount}`;
    } catch (error) {
      this.logger.error(`Unexpected error: ${error.message}`);
      throw new HttpException(
        `Schema detection failed: ${error.message}`,
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  private async detectCsvSchema(
    bucket: string,
    key: string,
    sampleLines: number,
  ): Promise<SchemaField[]> {
    const getObjectCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await this.s3Client.send(getObjectCommand);
    const stream = response.Body as Readable;
    if (!stream) throw new Error(`Empty stream for file: ${key}`);

    let header: string[] = [];
    const dataTypes: string[] = [];
    let linesParsed = 0;
    let stopStream = false;

    const parser = stream.pipe(
      parse({
        delimiter: ",",
        quote: '"',
        escape: "\\",
        columns: false,
        trim: true,
      }),
    );

    const transform = new Transform({
      objectMode: true,
      transform: (row: string[], _, callback) => {
        if (stopStream) return callback(); // skip after enough lines

        if (linesParsed === 0) {
          header = row;
          dataTypes.length = header.length;
          dataTypes.fill("string");
        } else if (linesParsed <= sampleLines) {
          for (let i = 0; i < Math.min(row.length, header.length); i++) {
            const val = row[i];
            if (val === "") continue;
            if (/^-?\d+$/.test(val)) {
              dataTypes[i] = "int";
            } else if (/^-?\d*\.\d+$/.test(val)) {
              dataTypes[i] = "double";
            }
          }
        }

        linesParsed++;
        if (linesParsed > sampleLines) {
          stopStream = true; // stop parsing new rows
        }

        callback(null, row);
      },
    });

    parser.pipe(transform);

    await new Promise<void>((resolve, reject) => {
      transform.on("finish", resolve);
      transform.on("error", reject);
      parser.on("error", reject);
      stream.on("error", reject);
    });

    return header.map((name, i) => ({
      name,
      type: dataTypes[i] || "string",
    }));
  }

  // New optimized bulk insert
  private async insertSchemaIntoDatabaseBulk(
    tableName: string,
    schemaName: string,
    schema: SchemaField[],
  ): Promise<void> {
    const uniqueColumns = new Map<string, SchemaField>();

    for (const field of schema) {
      const columnName = field.name?.trim();
      if (!columnName) continue; // Skip empty names

      if (!uniqueColumns.has(columnName)) {
        uniqueColumns.set(columnName, { name: columnName, type: field.type });
      }
    }

    if (uniqueColumns.size === 0) {
      this.logger.warn(
        `No valid or unique columns found for table "${tableName}" in schema "${schemaName}". Skipping insert.`,
      );
      return;
    }

    const records = Array.from(uniqueColumns.values()).map((field) =>
      this.columnRepo.create({
        table_name: tableName,
        schema_name: schemaName,
        column_name: field.name,
        type: field.type,
      }),
    );

    try {
      await this.columnRepo.save(records);
      this.logger.log(
        `Inserted ${records.length} unique columns for ${tableName} in schema ${schemaName}`,
      );
    } catch (error) {
      this.logger.error(
        `Error inserting schema for ${tableName} in schema ${schemaName}: ${error.message}`,
      );
      throw new HttpException(
        `DB insert failed for ${tableName} in schema ${schemaName}: ${error.message}`,
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  async createTablesInAthena(schemaName: string): Promise<any> {
    const schemaTables = await this.getSchemaColumns(schemaName);
    const results = [];

    for (const table of schemaTables) {
      const createQuery = this.buildCreateTableQuery(
        schemaName,
        table.tableName,
        table.columns,
      );

      try {
        await this.athenaService.executeQuery(createQuery, schemaName, true); // Use rawOutput = true for DDL
        results.push({ table: table.tableName, status: "submitted" });
      } catch (error) {
        results.push({
          table: table.tableName,
          status: "failed",
          error: error.message,
        });
      }
    }

    return {
      message: "Tables creation queries submitted to Athena",
      results,
    };
  }

  private buildCreateTableQuery(
    schema: string,
    tableName: string,
    columns: { name: string; type: string }[],
  ): string {
    const columnDefs = columns
      .map((col) => `\`${col.name}\` ${col.type}`)
      .join(",\n  ");
    return `
      CREATE TABLE IF NOT EXISTS hospital_data.${tableName} (
        ${columnDefs}
      )
      LOCATION 's3://bk-health-bucket-trusted/'
      TBLPROPERTIES (
          'table_type'='ICEBERG',
          'format'='parquet'
      );
    `.trim();
  }
}
