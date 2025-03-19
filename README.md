# RPC Indexer

A block watcher service that indexes Ethereum blockchain data into PostgreSQL databases.

## Overview

This project watches Ethereum blocks and stores relevant data in PostgreSQL databases. It's built with TypeScript and uses Viem for Ethereum interaction. The indexer specifically:

- Indexes gas fee data from Ethereum blocks
- Aggregates time-weighted average price (TWAP) data
- Processes unfinalized data received directly from an RPC endpoint

## Prerequisites

- Node.js 18+
- PostgreSQL
- Docker and Docker Compose (for containerized deployment)
- Ethereum RPC endpoint

## Installation

### Local Development

1. Clone the repository
2. Install dependencies:
   ```
   npm install
   ```
3. Create a `.env` file based on the `.env.example` in the src directory
4. Build the TypeScript code:
   ```
   npm run build
   ```
5. Start the service:
   ```
   npm start
   ```

### Docker Deployment

1. Configure environment variables in `.env` file or docker-compose.yml
2. Build and run with Docker Compose:
   ```
   docker-compose up -d
   ```

## Configuration

The service requires the following environment variables:

- `DATABASE_URL`: PostgreSQL connection string for the indexer database
- `FOSSIL_DB_URL`: PostgreSQL connection string for the fossil database
- `MAINNET_RPC_URL`: Ethereum mainnet RPC URL

## Project Structure

- `src/index.ts`: Main entry point
- `src/runner.ts`: Block watching and processing logic
- `src/db.ts`: Database interaction 