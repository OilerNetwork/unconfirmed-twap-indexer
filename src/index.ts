import { Alchemy, Network } from "alchemy-sdk";
import { Pool } from "pg";
import * as dotenv from "dotenv";
import { initializeTWAPState, updateTWAPState } from "./db";

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

// Remove the TWAPInitStatus interface and variable since we'll use the database
interface TWAPState {
  weightedSum: number;
  totalSeconds: number;
  twapValue: number;
  lastBlockNumber: number;
  lastBlockTimestamp: number;
}

const INITIAL_BLOCK = process.env.INITIAL_BLOCK ? 
  parseInt(process.env.INITIAL_BLOCK) : 
  0; // Default to 0 if not specified

let isShuttingDown = false;

async function getInitialState(): Promise<number> {
  // If INITIAL_BLOCK is set, and there's no existing state, use it
  console.log('INITIAL_BLOCK:', INITIAL_BLOCK);
  if (INITIAL_BLOCK > 0) {
    const query = `
      SELECT COUNT(*) as count
      FROM twap_state 
      WHERE NOT is_confirmed
    `;
    
    const result = await pool.query(query);
    console.log('Current state count:', result.rows[0].count);
    
    if (Number(result.rows[0].count) === 0) {
      console.log(`No existing state found, initializing with INITIAL_BLOCK: ${INITIAL_BLOCK}`);
      await Promise.all([
        initializeTWAPState(pool, 'twelve_min', INITIAL_BLOCK),
        initializeTWAPState(pool, 'three_hour', INITIAL_BLOCK),
        initializeTWAPState(pool, 'thirty_day', INITIAL_BLOCK)
      ]);
      
      return INITIAL_BLOCK;
    } else {
      console.log('Found existing state, using last processed block');
    }
  }

  // If we have existing state, get the most recent unconfirmed state
  const query = `
    SELECT last_block_number 
    FROM twap_state 
    WHERE NOT is_confirmed 
    ORDER BY last_block_number DESC 
    LIMIT 1
  `;

  const result = await pool.query(query);
  
  if (result.rows.length > 0) {
    // Resume from the last processed block
    return result.rows[0].last_block_number;
  }

  // If no state exists and no INITIAL_BLOCK set, start from 0
  await Promise.all([
    initializeTWAPState(pool, 'twelve_min', INITIAL_BLOCK),
    initializeTWAPState(pool, 'three_hour', INITIAL_BLOCK),
    initializeTWAPState(pool, 'thirty_day', INITIAL_BLOCK)
  ]);

  return 0;
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

async function handleNewBlock(blockNumber: number) {
  try {
    const block = await alchemy.core.getBlock(blockNumber);
    if (!block.baseFeePerGas) {
      console.log(`Block ${blockNumber} has no base fee, skipping`);
      return;
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

    await pool.query('BEGIN');

    try {
      // Store block with TWAPs
      await pool.query(`
        INSERT INTO blocks (
          block_number, timestamp, basefee, is_confirmed,
          twelve_min_twap, three_hour_twap, thirty_day_twap
        ) VALUES ($1, $2, $3, false, $4, $5, $6)
        ON CONFLICT (block_number) 
        DO UPDATE SET 
          basefee = EXCLUDED.basefee,
          timestamp = EXCLUDED.timestamp,
          twelve_min_twap = EXCLUDED.twelve_min_twap,
          three_hour_twap = EXCLUDED.three_hour_twap,
          thirty_day_twap = EXCLUDED.thirty_day_twap
        WHERE NOT blocks.is_confirmed
        RETURNING *
      `, [
        blockNumber,
        block.timestamp,
        basefee,
        twelveMin.twap,
        threeHour.twap,
        thirtyDay.twap,
      ]);

      // Update TWAP states
      await Promise.all([
        updateTWAPState(pool, 'twelve_min', twelveMin, blockNumber, block.timestamp),
        updateTWAPState(pool, 'three_hour', threeHour, blockNumber, block.timestamp),
        updateTWAPState(pool, 'thirty_day', thirtyDay, blockNumber, block.timestamp)
      ]);

      await pool.query('COMMIT');

      console.log(`Processed block ${blockNumber} with TWAPs:`, {
        twelveMinTwap: twelveMin.twap,
        threeHourTwap: threeHour.twap,
        thirtyDayTwap: thirtyDay.twap,
      });
    } catch (error) {
      await pool.query('ROLLBACK');
      console.error('Transaction error:', error);
      throw error;
    }
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
    const lastProcessedBlock = await getInitialState();
    console.log(`Last processed block: ${lastProcessedBlock}, Current chain head: ${currentBlock}`);

    // Catch up on missing blocks
    if (lastProcessedBlock < currentBlock) {
      console.log(`Catching up from block ${lastProcessedBlock + 1} to ${currentBlock}`);
      
      // Process blocks sequentially
      for (let blockNumber = Number(lastProcessedBlock) + 1; blockNumber <= currentBlock; blockNumber++) {
        try {
          console.log(`Processing block ${blockNumber}`);
          await handleNewBlock(blockNumber);
          if (blockNumber % 10 === 0) {
            console.log(`Processed block ${blockNumber}`);
          }
        } catch (error) {
          console.error(`Error processing block ${blockNumber}:`, error);
          throw error; // Stop processing on error
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
