import { Module } from "@nestjs/common";
import { S3Client } from "@aws-sdk/client-s3";

@Module({
  providers: [
    {
      provide: S3Client,
      useFactory: () => {
        return new S3Client({
          region: process.env.AWS_REGION,
          credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY,
            secretAccessKey: process.env.AWS_SECRET_KEY,
          },
        });
      },
    },
  ],
  exports: [S3Client], // Export S3Client so other modules can use it
})
export class S3Module {}
