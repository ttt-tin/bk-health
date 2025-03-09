import { Module } from "@nestjs/common";
import { S3Client } from "@aws-sdk/client-s3";
import { S3Service } from "./s3.service";
import { S3Controller } from "./s3.controller";

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
    S3Service,
  ],
  controllers: [S3Controller],
  exports: [S3Client, S3Service], // Export S3Client so other modules can use it
})
export class S3Module {}
