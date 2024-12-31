import { IsString, IsOptional } from 'class-validator';

export class UpdateMappingDto {
  @IsString()
  @IsOptional()
  dbName?: string;

  @IsString()
  @IsOptional()
  dbCloumn?: string;

  @IsString()
  @IsOptional()
  standardColumn?: string;
}
