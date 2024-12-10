import { Injectable } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';

@Injectable()
export class ConstraintService {
  async createConstraintFile(tableName: string, constraintCondition: string): Promise<string> {
    try {
      const decodedConstraintCondition = decodeURIComponent(constraintCondition);

      const fileName = `${tableName}_constraints.txt`;
      const filePath = path.join(__dirname, '../../holoclean/constraints', fileName);

      const content = `${decodedConstraintCondition}\n`;

      fs.mkdirSync(path.dirname(filePath), { recursive: true });

      fs.writeFileSync(filePath, content);

      return `Constraint file created successfully: ${fileName}`;
    } catch (error) {
      throw new Error('Failed to create constraint file');
    }
  }
}
