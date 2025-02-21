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
    ON CONFLICT ON CONSTRAINT twap_state_window_type_is_confirmed_key DO NOTHING
  `, [windowType, 0, 0, 0, initialBlock, 0]);
}

export async function updateBlockAndTWAPStates(
  pool: Pool,
  blockNumber: number,
  timestamp: number,
  basefee: number,
  twaps: {
    twelveMin: { weightedSum: number; totalSeconds: number; twap: number };
    threeHour: { weightedSum: number; totalSeconds: number; twap: number };
    thirtyDay: { weightedSum: number; totalSeconds: number; twap: number };
  }
): Promise<{ shouldRecalibrate: boolean; nextStartBlock?: number }> {
  await pool.query('BEGIN');
  
  try {
    // First check if this block is already confirmed
    const checkResult = await pool.query(`
      SELECT is_confirmed FROM blocks 
      WHERE block_number = $1 AND is_confirmed = true
    `, [blockNumber]);

    if (checkResult.rows.length > 0) {
      // Block is already confirmed, need to recalibrate
      const latestConfirmedBlock = await pool.query(`
        SELECT block_number 
        FROM blocks 
        WHERE is_confirmed = true 
        ORDER BY block_number DESC 
        LIMIT 1
      `);

      if (latestConfirmedBlock.rows.length > 0) {
        // Delete all unconfirmed TWAP states before recalibrating
        await pool.query(`
          DELETE FROM twap_state 
          WHERE NOT is_confirmed
        `);
        
        await pool.query('COMMIT');
        
        return {
          shouldRecalibrate: true,
          nextStartBlock: Number(latestConfirmedBlock.rows[0].block_number) + 1
        };
      }
    }

    // If not confirmed, proceed with normal update
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
    `, [
      blockNumber,
      timestamp,
      basefee,
      twaps.twelveMin.twap,
      twaps.threeHour.twap,
      twaps.thirtyDay.twap,
    ]);

    // Update TWAP states
    await pool.query(`
      INSERT INTO twap_state (window_type, weighted_sum, total_seconds, twap_value, last_block_number, last_block_timestamp, is_confirmed)
      VALUES 
        ($1, $2, $3, $4, $5, $6, false),
        ($7, $8, $9, $10, $5, $6, false),
        ($11, $12, $13, $14, $5, $6, false)
      ON CONFLICT ON CONSTRAINT twap_state_window_type_is_confirmed_key 
      DO UPDATE SET
        weighted_sum = EXCLUDED.weighted_sum,
        total_seconds = EXCLUDED.total_seconds,
        twap_value = EXCLUDED.twap_value,
        last_block_number = EXCLUDED.last_block_number,
        last_block_timestamp = EXCLUDED.last_block_timestamp
      WHERE twap_state.is_confirmed = false AND twap_state.window_type = EXCLUDED.window_type
    `, [
      'twelve_min', twaps.twelveMin.weightedSum, twaps.twelveMin.totalSeconds, twaps.twelveMin.twap,
      blockNumber, timestamp,
      'three_hour', twaps.threeHour.weightedSum, twaps.threeHour.totalSeconds, twaps.threeHour.twap,
      'thirty_day', twaps.thirtyDay.weightedSum, twaps.thirtyDay.totalSeconds, twaps.thirtyDay.twap
    ]);

    await pool.query('COMMIT');
    return { shouldRecalibrate: false };
  } catch (error) {
    await pool.query('ROLLBACK');
    throw error;
  }
}

