import { Controller, Get, Query } from '@nestjs/common';
import { PythonService } from './python.service';

@Controller('python')
export class PythonController {
  constructor(private readonly pythonService: PythonService) {}

  @Get('run')
  async runPython(
  ): Promise<string> {
    const result = await this.pythonService.runShellScript();

    console.log('Cleaning completed.')
    return result;
  }
}
