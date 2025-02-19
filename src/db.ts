import { Pool } from "pg";

export async function initializeTWAPState(
  pool: Pool, 
  windowType: string, 
  initialBlock: number
): Promise<void> {
  await pool.query(`
    INSERT INTO twap_state (
      window_type, 
      weighted_sum,
      total_seconds,
      twap_value,
      last_block_number, 
      last_block_timestamp, 
      is_confirmed
    ) VALUES ($1, $2, $3, $4, $5, $6, false)
    ON CONFLICT (window_type, is_confirmed) DO NOTHING
  `, [windowType, 0, 0, 0, initialBlock, 0]);
}

export async function updateTWAPState(
  pool: Pool,
  windowType: string,
  data: { weightedSum: number; totalSeconds: number; twap: number },
  blockNumber: number,
  timestamp: number
): Promise<void> {
  const result = await pool.query(`
    INSERT INTO twap_state (
      window_type, weighted_sum, total_seconds, twap_value,
      last_block_number, last_block_timestamp, is_confirmed
    ) VALUES ($1, $2, $3, $4, $5, $6, false)
    ON CONFLICT (window_type, is_confirmed) 
    DO UPDATE SET
      weighted_sum = EXCLUDED.weighted_sum,
      total_seconds = EXCLUDED.total_seconds,
      twap_value = EXCLUDED.twap_value,
      last_block_number = EXCLUDED.last_block_number,
      last_block_timestamp = EXCLUDED.last_block_timestamp
    WHERE twap_state.is_confirmed = false
    RETURNING *
  `, [
    windowType,
    data.weightedSum,
    data.totalSeconds,
    data.twap,
    blockNumber,
    timestamp
  ]);

  if (result.rows.length === 0) {
    console.error(`No update/insert occurred for ${windowType} TWAP state. Possible conflict with confirmed state.`);
  } else {
    console.log(`Updated ${windowType} TWAP state:`);
  }
}

