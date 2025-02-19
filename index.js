import { WorkerService } from './worker.js';

async function main() {
  const worker = new WorkerService();

  try {
    await worker.initialize();
    console.log('Worker service started');

    // Handle graceful shutdown
    process.on('SIGTERM', async () => {
      console.log('Received SIGTERM signal');
      await worker.shutdown();
      process.exit(0);
    });
  } catch (error) {
    console.error('Failed to start worker service:', error);
    process.exit(1);
  }
}

main();
