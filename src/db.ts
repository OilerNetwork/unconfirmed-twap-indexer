import { PoolClient, Pool } from "pg";

// Add a function to get the latest block from FOSSIL DB
export async function getLatestFossilBlock(fossilPool: Pool): Promise<number> {
  const result = await fossilPool.query(`
    SELECT number AS block_number
    FROM blockheaders
    ORDER BY number DESC
    LIMIT 1
  `);

  if (result.rows.length === 0) {
    return 0;
  }

  return Number(result.rows[0].block_number);
}

// This function is no longer needed as we're using direct parameterized queries instead of prepared statements
export async function initializeTWAPState(
  client: PoolClient,
  windowType: string,
  initialBlock: number
): Promise<void> {
  await client.query(
    `
    INSERT INTO twap_state (
      window_type, 
      weighted_sum,
      total_seconds,
      twap_value,
      last_block_number, 
      last_block_timestamp, 
      is_confirmed    
    ) VALUES ($1::twap_window_type, $2, $3, $4, $5, $6, false)
    ON CONFLICT ON CONSTRAINT twap_state_window_type_is_confirmed_key DO UPDATE SET
      weighted_sum = EXCLUDED.weighted_sum,
      total_seconds = EXCLUDED.total_seconds,
      twap_value = EXCLUDED.twap_value,
      last_block_number = EXCLUDED.last_block_number,
      last_block_timestamp = EXCLUDED.last_block_timestamp,
      is_confirmed = false
  `,
    [windowType, 0, 0, 0, initialBlock, 0]
  );
}

export async function updateBlockAndTWAPStates(
  client: PoolClient,
  blockNumber: number,
  timestamp: number,
  basefee: number,
  twaps: {
    twelveMin: { weightedSum: number; totalSeconds: number; twap: number };
    threeHour: { weightedSum: number; totalSeconds: number; twap: number };
    thirtyDay: { weightedSum: number; totalSeconds: number; twap: number };
  }
): Promise<{ shouldRecalibrate: boolean }> {
  await client.query("BEGIN");

  try {
    // Use direct parameterized query instead of prepared statement
    const blockInsertResult = await client.query(
      `
      WITH block_update AS (
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
        RETURNING blocks.is_confirmed
      )
      SELECT * FROM block_update
      `,
      [
        blockNumber,
        timestamp,
        basefee,
        twaps.twelveMin.twap,
        twaps.threeHour.twap,
        twaps.thirtyDay.twap,
      ]
    );

    // If the block was confirmed, trigger recalibration
    if (
      blockInsertResult.rows.length === 0 ||
      blockInsertResult.rows[0].is_confirmed
    ) {
      client.query("ROLLBACK");
      return {
        shouldRecalibrate: true,
      };
    }

    // Use direct parameterized query for TWAP updates
    await client.query(
      `
      INSERT INTO twap_state (window_type, weighted_sum, total_seconds, twap_value, last_block_number, last_block_timestamp, is_confirmed)
      VALUES 
        ($1::twap_window_type, $2, $3, $4, $5, $6, false),
        ($7::twap_window_type, $8, $9, $10, $5, $6, false),
        ($11::twap_window_type, $12, $13, $14, $5, $6, false)
      ON CONFLICT ON CONSTRAINT twap_state_window_type_is_confirmed_key 
      DO UPDATE SET
        weighted_sum = EXCLUDED.weighted_sum,
        total_seconds = EXCLUDED.total_seconds,
        twap_value = EXCLUDED.twap_value,
        last_block_number = EXCLUDED.last_block_number,
        last_block_timestamp = EXCLUDED.last_block_timestamp
      WHERE twap_state.is_confirmed = false AND twap_state.window_type = EXCLUDED.window_type::twap_window_type
      `,
      [
        "twelve_min",
        twaps.twelveMin.weightedSum,
        twaps.twelveMin.totalSeconds,
        twaps.twelveMin.twap,
        blockNumber,
        timestamp,
        "three_hour",
        twaps.threeHour.weightedSum,
        twaps.threeHour.totalSeconds,
        twaps.threeHour.twap,
        "thirty_day",
        twaps.thirtyDay.weightedSum,
        twaps.thirtyDay.totalSeconds,
        twaps.thirtyDay.twap,
      ]
    );

    await client.query("COMMIT");
    return { shouldRecalibrate: false };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}
