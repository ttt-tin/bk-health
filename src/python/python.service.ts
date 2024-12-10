import { Injectable } from '@nestjs/common';
import { spawn } from 'child_process';

@Injectable()
export class PythonService {
  async runPythonScript(args: string[]): Promise<string> {
    return new Promise((resolve, reject) => {
      const scriptPath = '../script.py';

      const process = spawn('python', [scriptPath, ...args], { shell: true });

      let output = '';
      let error = '';

      process.stdout.on('data', (data) => {
        output += data.toString();
      });

      process.stderr.on('data', (data) => {
        error += data.toString();
      });

      process.on('close', (code) => {
        if (code === 0) {
          resolve(output.trim());
        } else {
          reject(new Error(`Python script exited with code ${code}: ${error}`));
        }
      });
    });
  }

  async runShellScript(arg: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const scriptPath = './holoclean/run/script.sh';

      const process = spawn('bash', [scriptPath, arg]);

      let output = '';
      let error = '';

      process.stdout.on('data', (data) => {
        output += data.toString();
      });

      process.stderr.on('data', (data) => {
        error += data.toString();
      });

      process.on('close', (code) => {
        if (code === 0) {
          resolve(output.trim());
        } else {
          reject(new Error(`Shell script exited with code ${code}: ${error}`));
        }
      });
    });
  }
}
