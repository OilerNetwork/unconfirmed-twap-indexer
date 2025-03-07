import { Alchemy, Network, Block as AlchemyBlock } from "alchemy-sdk";
import { Pool } from "pg";
import * as dotenv from "dotenv";
import { initializeTWAPState, updateBlockAndTWAPStates } from "./db";

dotenv.config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const fossilPool = new Pool({
  connectionString: process.env.FOSSIL_DB_URL,
  ssl: {
    rejectUnauthorized: false
  }
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
    initializeTWAPState(pool,  'twelve_min', startBlock),
    initializeTWAPState(pool,  'three_hour', startBlock),
    initializeTWAPState(pool,  'thirty_day', startBlock)
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

async function handleNewBlock(block:AlchemyBlock): Promise<boolean> {
  try {
    if (!block.baseFeePerGas) {
      console.log(`Block ${block.number} has no base fee, skipping`);
      return false;
    }

    const basefee = Number(block.baseFeePerGas.toString());
    const currentBlock = {
      number: block.number,
      timestamp: block.timestamp,
      basefee: basefee
    };

    // Calculate TWAPs
    const [twelveMin, threeHour, thirtyDay] = await Promise.all([
      calculateTWAP(pool, TWAP_RANGES.TWELVE_MIN, currentBlock),
      calculateTWAP(pool, TWAP_RANGES.THREE_HOURS, currentBlock),
      calculateTWAP(pool, TWAP_RANGES.THIRTY_DAYS, currentBlock),
    ]);

    const result = await updateBlockAndTWAPStates(pool, fossilPool, block.number, block.timestamp, basefee, {
      twelveMin,
      threeHour,
      thirtyDay
    });

    if (result.shouldRecalibrate && result.nextStartBlock) {
      console.log(`Found confirmed block ${block.number}, recalibrating to start from ${result.nextStartBlock}`);
      // Reinitialize TWAP states with new starting block
      await Promise.all([
        initializeTWAPState(pool, 'twelve_min', result.nextStartBlock),
        initializeTWAPState(pool, 'three_hour', result.nextStartBlock),
        initializeTWAPState(pool, 'thirty_day', result.nextStartBlock)
      ]);
      return true; // Signal that we need to recalibrate
    }

    console.log(`Processed block ${block.number}`);
    return false;
  } catch (error) {
    console.error(`Error processing block ${block.number}:`, error);
    throw error;
  }
}

// Add retry logic at the top
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
  try {
    console.log("Starting block watcher...");
    
    // Get current chain head
    let currentBlock = await alchemy.core.getBlockNumber();
    
    // Get the last processed block from our state
    const lastProcessedBlock = Number(await getInitialState());
    console.log(`Last processed block: ${lastProcessedBlock}, Current chain head: ${currentBlock}`);

    // Catch up on missing blocks
    if (lastProcessedBlock < Number(currentBlock)) {
      console.log(`Catching up from block ${lastProcessedBlock + 1} to ${currentBlock}`);
      
      let blockNumber = lastProcessedBlock;
      while (blockNumber < currentBlock) {
        try {
          const length = Math.min(currentBlock - blockNumber, 1000);
          console.log("LENGTH", length);
          const promises = Array.from({length}, async (_, i) => {
          console.log("BLOCK NUMBER", blockNumber + i);
            const block = await alchemy.core.getBlock(blockNumber + i);
            return block;
          });
          const blocks = await Promise.all(promises);
          blocks.sort((a, b) => a.number - b.number);
          blocks.forEach(block => {
            console.log("BLOCK", block.number);
          });

          for (const block of blocks) {
            while (true) {
              try {
                const needsRecalibration = await handleNewBlock(block);
                if (needsRecalibration) {
                  return main(); // Start over from getInitialState
                }
                break; // Success, move to next block
              } catch (error) {
                console.error(`Error processing block ${block.number}, retrying:`, error);
                await sleep(1000); // Wait a second before retrying
              }
            }
          }
          
          currentBlock = await alchemy.core.getBlockNumber();
          blockNumber += length;
        } catch (error) {
          console.error(`Error fetching blocks at ${blockNumber}:`, error);
          await sleep(1000); // Wait before retrying the batch
          continue;
        }
      }
      console.log('Caught up with historical blocks');
    }

    // Now start watching for new blocks
    console.log('Starting real-time block watching');
    
    // Create a reference to keep the process alive
    
    console.log("REACHED HERE");
      alchemy.ws.on("block", async (blockNumber) => {
        while (true) {
          try {
            const block = await alchemy.core.getBlock(blockNumber);
            console.log("BLOCK", block.number);
            await handleNewBlock(block);
            break;
          } catch (error) {
            console.error(`Error processing block ${blockNumber}, retrying:`, error);
            await sleep(1000);
          }
        }
      });
      // Clean up on shutdown
      process.once('SIGINT', () => {
        shutdown();
      });
      process.once('SIGTERM', () => {
        shutdown();
      });

  } catch (error) {
    console.error("Error starting watcher:", error);
    throw error;
  }
}

async function shutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  
  console.log('Closing database pool...');
  try {
    await pool.end();
    await fossilPool.end();
    console.log('Database pool closed');
  } catch (error) {
    console.error('Error closing database pool:', error);
  }
  process.exit(0);
}

main().catch((error) => {
  console.error("Error in main:", error);
  process.exit(1);
});
