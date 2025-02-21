import { Alchemy, Network } from "alchemy-sdk";
import { Pool } from "pg";
import * as dotenv from "dotenv";
import { initializeTWAPState, updateBlockAndTWAPStates } from "./db";

dotenv.config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const config = {
  apiKey: process.env.ALCHEMY_API_KEY,
  network: Network.ETH_MAINNET,
};
const alchemy = new Alchemy(config);

interface Block {
  block_number: number;
  timestamp: number;
  basefee: number;
  is_confirmed: boolean;
  twelve_min_twap: number | null;
  three_hour_twap: number | null;
  thirty_day_twap: number | null;
}

// Time ranges in seconds (moved from GasDataService)
const TWAP_RANGES = {
  TWELVE_MIN: 12 * 60,
  THREE_HOURS: 3 * 60 * 60,
  THIRTY_DAYS: 30 * 24 * 60 * 60,
} as const;


const INITIAL_BLOCK = process.env.INITIAL_BLOCK ? 
  parseInt(process.env.INITIAL_BLOCK) : 
  0; // Default to 0 if not specified

let isShuttingDown = false;

async function getInitialState(): Promise<number> {
  // First check if we have any unconfirmed TWAP state
  const twapStateQuery = `
    SELECT last_block_number 
    FROM twap_state 
    WHERE NOT is_confirmed 
    ORDER BY last_block_number DESC 
    LIMIT 1
  `;

  const twapResult = await pool.query(twapStateQuery);
  
  if (twapResult.rows.length > 0) {
    // Resume from the last processed block in TWAP state
    console.log("RESUME",twapResult.rows[0].last_block_number);
    return twapResult.rows[0].last_block_number;
  }

  // If no TWAP state exists, find the highest confirmed block
  const confirmedBlockQuery = `
    SELECT block_number 
    FROM blocks 
    WHERE is_confirmed = true 
    ORDER BY block_number DESC 
    LIMIT 1
  `;

  const blockResult = await pool.query(confirmedBlockQuery);
  const startBlock = blockResult.rows.length > 0 ? 
    Number(blockResult.rows[0].block_number) + 1 : 
    Number(INITIAL_BLOCK);

  console.log(`No TWAP state found. Starting from block ${startBlock}`);

  // Initialize TWAP states with the starting block
  await Promise.all([
    initializeTWAPState(pool, 'twelve_min', startBlock),
    initializeTWAPState(pool, 'three_hour', startBlock),
    initializeTWAPState(pool, 'thirty_day', startBlock)
  ]);

  return startBlock;
}

async function calculateTWAP(
  db: Pool,
  timeWindow: number,
  currentBlock: { number: number; timestamp: number; basefee: number }
): Promise<{ twap: number; weightedSum: number; totalSeconds: number }> {
  // Fetch relevant blocks for the window
  const query = `
    WITH time_windows AS (
      SELECT 
        block_number,
        timestamp::numeric,
        LEAD(timestamp::numeric) OVER (ORDER BY timestamp ASC) as next_timestamp,
        basefee::numeric
      FROM blocks
      WHERE timestamp >= ($1::numeric - $2::numeric)  
        AND timestamp <= $1::numeric
      ORDER BY timestamp ASC
    )
    SELECT 
      timestamp,
      next_timestamp,
      basefee
    FROM time_windows
    WHERE next_timestamp IS NOT NULL
  `;

  const result = await db.query(query, [
    currentBlock.timestamp,
    timeWindow
  ]);

  const blocks = result.rows;

  // If no historical blocks, use current block's basefee and full window duration
  if (blocks.length === 0) {
    return {
      twap: currentBlock.basefee,
      weightedSum: currentBlock.basefee * timeWindow,
      totalSeconds: timeWindow
    };
  }

  let weightedSum = 0;
  let totalSeconds = 0;

  // Calculate weighted sum
  for (const block of blocks) {
    const duration = Number(block.next_timestamp) - Number(block.timestamp);
    weightedSum += Number(block.basefee) * duration;
    totalSeconds += duration;
  }

  // Calculate TWAP
  const twap = totalSeconds > 0 ? weightedSum / totalSeconds : currentBlock.basefee;

  return {
    twap,
    weightedSum,
    totalSeconds
  };
}

async function handleNewBlock(blockNumber: number): Promise<boolean> {
  try {
    const block = await alchemy.core.getBlockWithTransactions(blockNumber);
    if (!block.baseFeePerGas) {
      console.log(`Block ${blockNumber} has no base fee, skipping`);
      return false;
    }

    const basefee = Number(block.baseFeePerGas.toString());
    const currentBlock = {
      number: blockNumber,
      timestamp: block.timestamp,
      basefee: basefee
    };

    // Calculate TWAPs
    const [twelveMin, threeHour, thirtyDay] = await Promise.all([
      calculateTWAP(pool, TWAP_RANGES.TWELVE_MIN, currentBlock),
      calculateTWAP(pool, TWAP_RANGES.THREE_HOURS, currentBlock),
      calculateTWAP(pool, TWAP_RANGES.THIRTY_DAYS, currentBlock),
    ]);

    const result = await updateBlockAndTWAPStates(pool, blockNumber, block.timestamp, basefee, {
      twelveMin,
      threeHour,
      thirtyDay
    });

    if (result.shouldRecalibrate && result.nextStartBlock) {
      console.log(`Found confirmed block ${blockNumber}, recalibrating to start from ${result.nextStartBlock}`);
      // Reinitialize TWAP states with new starting block
      await Promise.all([
        initializeTWAPState(pool, 'twelve_min', result.nextStartBlock),
        initializeTWAPState(pool, 'three_hour', result.nextStartBlock),
        initializeTWAPState(pool, 'thirty_day', result.nextStartBlock)
      ]);
      return true; // Signal that we need to recalibrate
    }

    console.log(`Processed block ${blockNumber}`);
    return false;
  } catch (error) {
    console.error(`Error processing block ${blockNumber}:`, error);
    throw error;
  }
}

async function main() {
  try {
    console.log("Starting block watcher...");
    
    // Get current chain head
    const currentBlock = await alchemy.core.getBlockNumber();
    
    // Get the last processed block from our state
    const lastProcessedBlock = Number(await getInitialState());
    console.log(`Last processed block: ${lastProcessedBlock}, Current chain head: ${currentBlock}`);

    // Catch up on missing blocks
    if (lastProcessedBlock < Number(currentBlock)) {
      console.log(`Catching up from block ${lastProcessedBlock + 1} to ${currentBlock}`);
      
      // Process blocks sequentially
      for (let blockNumber = lastProcessedBlock; blockNumber <= currentBlock; blockNumber++) {
        try {
          console.log(`Processing block ${blockNumber}`);
          const needsRecalibration = await handleNewBlock(blockNumber);
          if (needsRecalibration) {
            // Start over from getInitialState
            return main();
          }
        } catch (error) {
          console.error(`Error processing block ${blockNumber}:`, error);
          throw error;
        }
      }
      console.log('Caught up with historical blocks');
    }

    // Now start watching for new blocks
    console.log('Starting real-time block watching');
    alchemy.ws.on("block", async (blockNumber) => {
      try {
        await handleNewBlock(blockNumber);
      } catch (error) {
        console.error(`Error processing block ${blockNumber}:`, error);
      }
    });
  } catch (error) {
    console.error("Error starting watcher:", error);
    process.exit(1);
  }
}

async function shutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  
  console.log('Closing database pool...');
  try {
    await pool.end();
    console.log('Database pool closed');
  } catch (error) {
    console.error('Error closing database pool:', error);
  }
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((error) => {
  console.error("Error in main:", error);
  process.exit(1);
});
