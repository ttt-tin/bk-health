import { IsNotEmpty, IsString } from 'class-validator';

export class CreateRelationshipDto {
  @IsNotEmpty()
  @IsString()
  tableReference: string;

  @IsNotEmpty()
  @IsString()
  tableWasReference: string;

  @IsNotEmpty()
  @IsString()
  priKey: string;

  @IsNotEmpty()
  @IsString()
  foKey: string;
}
