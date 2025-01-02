/* eslint-disable prettier/prettier */
import { Injectable } from '@nestjs/common';
import { spawn } from 'child_process';
import { HistoryEntity } from './entities/history-run.entity';
import { Repository } from 'typeorm';
import { InjectRepository } from '@nestjs/typeorm';

@Injectable()
export class PythonService {

  constructor(
    @InjectRepository(HistoryEntity) private historyRepository: Repository<HistoryEntity>,
  ) {}

  async runPythonScript(): Promise<string> {
    return new Promise((resolve, reject) => {
      const scriptPath = 'holoclean/extension/pipe.py';

      const process = spawn('python', [scriptPath], { shell: true });

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

  async runShellScript(): Promise<string> {
    const startTime = new Date();
    let status = 'Running';
    let duration: number;

    const historyEntry = this.historyRepository.create({
      startTime,
      duration: null, 
      status,
    });

    const savedHistoryEntry = await this.historyRepository.save(historyEntry);

    return new Promise((resolve, reject) => {
      const scriptPath = './holoclean/run/script.sh';
      const process = spawn('bash', [scriptPath]);

      let output = '';
      let error = '';

      process.stdout.on('data', (data) => {
        output += data.toString();
        console.log(data.toString());
      });

      process.stderr.on('data', (data) => {
        error += data.toString();
        console.log(data.toString());
      });

      process.on('close', async (code) => {
        const endTime = new Date();
        duration = Math.floor((endTime.getTime() - startTime.getTime()) / 1000); 

        if (code === 0) {
          status = 'Completed';
        } else {
          status = 'Error';
        }

        await this.historyRepository.update(savedHistoryEntry.id, {
          status,
          duration,
        });

        if (code === 0) {
          resolve(output.trim());
        } else {
          reject(new Error(`Shell script exited with code ${code}: ${error}`));
        }
      });
    });
  }
}
