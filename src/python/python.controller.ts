import { Controller, Get, Query } from '@nestjs/common';
import { PythonService } from './python.service';

@Controller('python')
export class PythonController {
  constructor(private readonly pythonService: PythonService) {}

  @Get('run')
  async runPython(
    @Query('arg') arg: string, 
  ): Promise<string> {
    return this.pythonService.runShellScript(arg);
  }
}
