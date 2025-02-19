import { Alchemy, Network } from 'alchemy-sdk';
import pg from 'pg';
import * as dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config();

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const config = {
  apiKey: process.env.ALCHEMY_API_KEY,
  network: Network.ETH_MAINNET,
};
const alchemy = new Alchemy(config);

async function handleNewBlock(blockNumber: number) {
  try {
    const block = await alchemy.core.getBlock(blockNumber);
    
    await pool.query(
      'INSERT INTO blocks (block_number, basefee, timestamp, is_confirmed) VALUES ($1, $2, $3, TRUE)',
      [
        block.number,
        block.baseFeePerGas?.toString() || '0',
        block.timestamp
      ]
    );

    console.log(`Stored block ${blockNumber} with base fee ${block.baseFeePerGas}`);
  } catch (error) {
    console.error(`Error processing block ${blockNumber}:`, error);
  }
}

async function main() {
  try {
    console.log('Starting block watcher...');
    // Watch for new blocks
    alchemy.ws.on('block', handleNewBlock);
  } catch (error) {
    console.error('Error starting watcher:', error);
    process.exit(1);
  }
}

// Handle cleanup on exit
process.on('SIGINT', async () => {
  console.log('Closing database pool...');
  await pool.end();
  process.exit();
});

main()
  .catch((error) => {
    console.error('Error in main:', error);
    process.exit(1);
  }); 