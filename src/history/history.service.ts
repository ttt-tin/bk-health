import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { HistoryEntity } from '../python/entities/history-run.entity';

@Injectable()
export class HistoryService {
  constructor(
    @InjectRepository(HistoryEntity)
    private readonly historyRepository: Repository<HistoryEntity>,
  ) {}

  async getHistory(limit?: number): Promise<HistoryEntity[]> {
    const query = this.historyRepository.createQueryBuilder('history');

    if (limit) {
      query.limit(limit);
    }

    return query.orderBy('history.id', 'DESC').getMany();
  }
}
