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

  async getTablesInSchema(schemaName: string): Promise<string[]> {
    try {
      const tables = await this.columnRepo
        .createQueryBuilder("column")
        .select("DISTINCT column.table_name", "table_name")
        .where("column.schema_name = :schemaName", { schemaName })
        .andWhere("column.table_name IS NOT NULL")
        .getRawMany();

      return tables.map((t) => t.table_name);
    } catch (error) {
      this.logger.error(
        `Failed to fetch tables for schema ${schemaName}: ${error.message}`,
      );
      throw new HttpException(
        `Failed to fetch tables for schema ${schemaName}: ${error.message}`,
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  async getTableSchema(
    schemaName: string,
    tableName: string,
  ): Promise<{ columns: { name: string; type: string }[] }> {
    try {
      const columns = await this.columnRepo.find({
        where: {
          schema_name: schemaName,
          table_name: tableName,
        },
      });

      if (!columns.length) {
        this.logger.warn(
          `No columns found for table ${schemaName}.${tableName}`,
        );
        throw new HttpException(
          `No columns found for table ${schemaName}.${tableName}`,
          HttpStatus.NOT_FOUND,
        );
      }

      return {
        columns: columns.map((col) => ({
          name: col.column_name,
          type: col.type || "string", // Fallback to "string" if type is null
        })),
      };
    } catch (error) {
      this.logger.error(
        `Failed to fetch schema for table ${schemaName}.${tableName}: ${error.message}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            `Failed to fetch schema for table ${schemaName}.${tableName}: ${error.message}`,
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
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

  async detectSchemas(bucket: string, sampleLines = 10): Promise<string> {
    try {
      const listObjectsCommand = new ListObjectsV2Command({
        Bucket: bucket,
      });

      const { Contents } = await this.s3Client.send(listObjectsCommand);
      if (!Contents || Contents.length === 0) {
        this.logger.log(`No objects found in bucket "${bucket}".`);
        return "No objects found.";
      }

      const csvFiles = Contents.filter((obj) => obj.Key?.endsWith(".csv"));

      const schemaTableFiles: Record<string, Record<string, string>> = {};

      for (const obj of csvFiles) {
        const key = obj.Key!;
        const parts = key.split("/");

        // Expecting: <schema>/<table>/<year>/<month>/<day>/<hour>/file.csv
        if (parts.length < 3) continue;

        const schemaName = parts[0];
        const tableName = parts[1];

        if (!schemaName || !tableName) continue;

        if (!schemaTableFiles[schemaName]) {
          schemaTableFiles[schemaName] = {};
        }

        // Only store one file path per schema+table
        if (!schemaTableFiles[schemaName][tableName]) {
          schemaTableFiles[schemaName][tableName] = key;
        }
      }

      let successCount = 0;
      let failCount = 0;

      for (const [schemaName, tables] of Object.entries(schemaTableFiles)) {
        for (const [tableName, key] of Object.entries(tables)) {
          const schemaExists = await this.columnRepo.findOne({
            where: { table_name: tableName, schema_name: schemaName },
          });

          if (schemaExists) {
            this.logger.log(
              `Schema already exists for table "${tableName}" in schema "${schemaName}". Skipping.`,
            );
            continue;
          }

          try {
            this.logger.log(`Detecting schema from: ${key}`);
            const schema = await this.detectCsvSchema(bucket, key, sampleLines);
            await this.insertSchemaIntoDatabaseBulk(
              tableName,
              schemaName,
              schema,
            );
            this.logger.log(
              `✅ Inserted schema for: ${tableName} (schema: ${schemaName})`,
            );
            successCount++;
          } catch (error) {
            this.logger.error(`❌ Failed to process ${key}: ${error.message}`);
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
    let dataTypes: string[] = [];
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
        if (stopStream) return callback();

        if (linesParsed === 0) {
          header = row.map((col) => col.trim());
          dataTypes = Array(header.length).fill("string");
        } else if (linesParsed <= sampleLines) {
          for (let i = 0; i < Math.min(row.length, header.length); i++) {
            const val = row[i]?.trim();
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
          stopStream = true;
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

    // Final cleaning + return
    return header
      .map((name, i) => ({
        name: name?.trim(),
        type: dataTypes[i] || "string",
      }))
      .filter((f) => f.name); // Ensure no empty/null column names
  }

  private async insertSchemaIntoDatabaseBulk(
    tableName: string,
    schemaName: string,
    schema: SchemaField[],
  ): Promise<void> {
    this.logger.log(
      `🔍 Detected columns for ${schemaName}.${tableName}:`,
      schema,
    );

    const seen = new Set<string>();
    const records = [];

    for (const field of schema) {
      const columnName = field.name?.trim();
      if (!columnName) {
        this.logger.warn(
          `⚠️ Skipping column with invalid name in ${schemaName}.${tableName}`,
        );
        continue;
      }

      const key = `${schemaName}.${tableName}.${columnName}`;
      if (!seen.has(key)) {
        seen.add(key);

        records.push(
          this.columnRepo.create({
            table_name: tableName,
            schema_name: schemaName,
            column_name: columnName,
            type: field.type,
          }),
        );
      }
    }

    if (records.length === 0) {
      this.logger.warn(
        `🚫 No valid or unique columns found for table "${tableName}" in schema "${schemaName}". Skipping insert.`,
      );
      return;
    }

    try {
      await this.columnRepo.save(records);
      this.logger.log(
        `✅ Inserted ${records.length} columns for ${tableName} in schema ${schemaName}`,
      );
    } catch (error) {
      this.logger.error(
        `❌ Error inserting schema for ${tableName} in schema ${schemaName}: ${error.message}`,
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
    // Check if key_id exists
    const hasKeyId = columns.some((col) => col.name.toLowerCase() === "key_id");

    // If not, add it manually
    const finalColumns = hasKeyId
      ? columns
      : [{ name: "key_id", type: "string" }, ...columns];

    const columnDefs = finalColumns
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
