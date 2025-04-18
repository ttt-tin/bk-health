import { IsNotEmpty, IsEnum } from "class-validator";
import { DataType, FileType } from "../entities/data-source.entity";

export class CreateDataSourceDto {
  @IsNotEmpty()
  name: string;

  @IsEnum(DataType)
  dataType: DataType;

  @IsEnum(FileType)
  fileType: FileType;
}
