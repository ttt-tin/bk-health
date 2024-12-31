import { Injectable } from '@nestjs/common';
import { KinesisClient, PutRecordCommand } from '@aws-sdk/client-kinesis';

@Injectable()
export class KinesisService {
    private kinesisClient: KinesisClient;

    constructor() {
        this.kinesisClient = new KinesisClient({
            region: process.env.AWS_KINESIS_REGION,
            credentials: {
                accessKeyId: process.env.AWS_KINESIS_ACCESS_KEY,
                secretAccessKey: process.env.AWS_KINESIS_SECRET_KEY,
            },
        });
    }

    async sendDataToKinesis(data: Record<string, any>, partitionKey: string): Promise<void> {
        try {
            const dataBuffer = Buffer.from(JSON.stringify(data));
            const command = new PutRecordCommand({
                StreamName: process.env.KINESIS_STREAM_NAME,
                Data: dataBuffer,
                PartitionKey: partitionKey,
            });

            const response = await this.kinesisClient.send(command);
            console.log('Data sent to Kinesis:', response);
        } catch (error) {
            console.error('Failed to send data to Kinesis:', error);
        }
    }
}
