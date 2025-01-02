import { IsString, IsOptional } from 'class-validator';

export class CreateMappingDto {
  @IsString()
  @IsOptional()
  dbName?: string;

  @IsString()
  @IsOptional()
  dbTable?: string;

  @IsString()
  @IsOptional()
  dbColumn?: string;

  @IsString()
  @IsOptional()
  standardColumn?: string;
}
