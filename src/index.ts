import * as dotenv from "dotenv";
import { Runner } from "./runner";

dotenv.config();
async function main() {
  try {
    const runner = new Runner();
    await runner.initialize();
    runner.startListening();
    // Clean up on shutdown
    process.once("SIGINT", () => {
      runner.shutdown();
      process.exit(0);  
    });
    process.once("SIGTERM", () => {
      runner.shutdown();
      process.exit(0);
    });
  } catch (error) {
    console.error("Error starting watcher:", error);
    throw error;
  }
}
main().catch((error) => {
  console.error("Error in main:", error);
  process.exit(1);
});
