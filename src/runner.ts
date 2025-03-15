import {
  Block,
  createPublicClient,
  http,
  webSocket,
  WatchBlocksReturnType,
} from "viem";
import { PublicClient } from "viem";
import { DB } from "./db";
import { mainnet } from "viem/chains";

const TWAP_RANGES = {
  TWELVE_MIN: 12 * 60,
  THREE_HOURS: 3 * 60 * 60,
  THIRTY_DAYS: 30 * 24 * 60 * 60,
} as const;

export class Runner {
  private db: DB;
  private viemClient: PublicClient;
  private rpcClient: PublicClient;
  unwatch: WatchBlocksReturnType | undefined;

  constructor() {
    this.db = new DB();
    this.rpcClient = createPublicClient({
      chain: mainnet,
      transport: http(`https://eth-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`),
    });
    this.viemClient = createPublicClient({
      chain: mainnet,
      transport: webSocket(
        `wss://eth-sepolia.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`
      ),
    });
  }

  async sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async initialize(): Promise<boolean> {
    let currentBlock = Number(await this.rpcClient.getBlockNumber());

    const lastProcessedBlock = Number(
      await this.db.getLastProcessedBlock(currentBlock)
    );
    // Get the last processed block from our state

    console.log(
      `Last processed block: ${lastProcessedBlock}, Current chain head: ${currentBlock}`
    );

    // Catch up on missing blocks
    if (lastProcessedBlock < Number(currentBlock)) {
      console.log(
        `Catching up from block ${lastProcessedBlock + 1} to ${currentBlock}`
      );

      let blockNumber = Number(lastProcessedBlock);
      while (blockNumber < currentBlock) {
        try {
          const length = Math.min(currentBlock - blockNumber, 1000);
          const blocks = await this.getBlocks(blockNumber, length);
          for (const block of blocks) {
            console.log("Block", block.number);
            while (true) {
              try {
                const needsRecalibration = await this.handleNewBlock(block);
                if (needsRecalibration) {
                    console.log("THIS")
                  await this.recalibrate();
                  return true; // Start over from getInitialState
                }
                break; // Success, move to next block
              } catch (error) {
                console.error(
                  `Error processing block ${block.number}, retrying:`,
                  error
                );
                await this.sleep(1000); // Wait a second before retrying
              }
            }
            console.log("BlockDone", block.number);
          }

          currentBlock = Number(await this.rpcClient.getBlockNumber());
          blockNumber += length;
          console.log("Current block", currentBlock);
        } catch (error) {
          console.error(`Error fetching blocks at ${blockNumber}:`, error);
          await this.sleep(1000); // Wait before retrying the batch
          continue;
        }
      }
    }
    return false;
  }

  async getBlocks(fromBlock: number, length: number): Promise<Block[]> {
    const promises = Array.from({ length }, async (_, i) => {
      const block = await this.rpcClient.getBlock({
        blockNumber: BigInt(fromBlock + i),
      });
      return block;
    });
    const blocks = await Promise.all(promises);
    blocks.sort((a, b) => Number(a.number) - Number(b.number));
    return blocks;
  }

  async recalibrate() {
    const currentBlock = Number(await this.rpcClient.getBlockNumber());
    const latestFossilBlock = await this.db.getLatestFossilBlock();
    const latestBlock = latestFossilBlock ?? currentBlock;
    await Promise.all([
      this.db.initializeTWAPState("twelve_min", latestBlock),
      this.db.initializeTWAPState("three_hour", latestBlock),
      this.db.initializeTWAPState("thirty_day", latestBlock),
    ]);
  }

  startListening() {
    const unwatch = this.viemClient.watchBlocks({
      onBlock: async (block: Block) => {
        try {
          const shouldRecalibrate = await this.handleNewBlock(block);
          if (shouldRecalibrate) {
            unwatch();
            await this.initialize();
            this.startListening();
          }
        } catch (error) {
          console.error("Error handling new block:", error);
        }
      },
    });
    this.unwatch = unwatch;
  }

  async handleNewBlock(block: Block): Promise<boolean> {
    try {
      if (!block.baseFeePerGas) {
        console.log(`Block ${block.number} has no base fee, skipping`);
        return false;
      }

      const basefee = Number(block.baseFeePerGas.toString());
      const newBlock = {
        number: Number(block.number),
        timestamp: Number(block.timestamp),
        basefee: basefee,
      };

      // Calculate TWAPs
      const [twelveMin, threeHour, thirtyDay] = await Promise.all([
        this.calculateTWAP(TWAP_RANGES.TWELVE_MIN, newBlock),
        this.calculateTWAP(TWAP_RANGES.THREE_HOURS, newBlock),
        this.calculateTWAP(TWAP_RANGES.THIRTY_DAYS, newBlock),
      ]);

      const result = await this.db.updateBlockAndTWAPStates(
        Number(block.number),
        Number(block.timestamp),
        basefee,
        {
          twelveMin,
          threeHour,
          thirtyDay,
        }
      );

      if (result.shouldRecalibrate) return true; // Signal that we need to recalibrate

      console.log(`Processed block ${block.number}`);
      return false;
    } catch (error) {
      console.error(`Error processing block ${block.number}:`, error);
      throw error;
    }
  }

  async calculateTWAP(
    timeWindow: number,
    currentBlock: { number: number; timestamp: number; basefee: number }
  ): Promise<{ twap: number; weightedSum: number; totalSeconds: number }> {
    const blocks = await this.db.getRelevantBlocks(
      currentBlock.timestamp,
      timeWindow
    );
    // Fetch relevant blocks for the window
    // If no historical blocks, use current block's
    if (blocks.length === 0) {
      return {
        twap: currentBlock.basefee,
        weightedSum: currentBlock.basefee,
        totalSeconds: 12,
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
    const twap =
      totalSeconds > 0 ? weightedSum / totalSeconds : currentBlock.basefee;

    return {
      twap,
      weightedSum,
      totalSeconds,
    };
  }

  async shutdown() {
    if (this.unwatch) {
      this.unwatch();
      this.db.shutdown();
    }
  }
  async run() {}
}
