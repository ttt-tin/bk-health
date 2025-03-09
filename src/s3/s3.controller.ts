import { Controller, Get, HttpException, HttpStatus } from "@nestjs/common";
import { S3Service } from "./s3.service"; // Adjust the import path based on your project structure

@Controller("s3") // Base route for this controller
export class S3Controller {
  constructor(private readonly s3Service: S3Service) {}

  @Get("statistics")
  async getS3Statistics() {
    try {
      const statistics = await this.s3Service.getS3Statistics();
      return {
        status: "success",
        data: statistics,
      };
    } catch (error) {
      throw new HttpException(
        {
          status: "error",
          message: "Failed to retrieve S3 statistics",
          error: error.message,
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
}
