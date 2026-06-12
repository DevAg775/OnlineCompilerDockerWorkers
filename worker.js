import amqp from 'amqplib';
import Docker from 'dockerode';
import { promises as fs } from 'fs';
import path from 'path';
import { PassThrough } from 'stream';
import { v4 as uuidv4 } from 'uuid';
import WebSocket, { WebSocketServer } from 'ws';
import { fileURLToPath } from 'url';
import { connectMongo, Execution } from './db.js'

// Get __dirname equivalent in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class WorkerService {
  constructor() {
    this.docker = new Docker();
    this.connection = null;
    this.channel = null;
    this.wss = new WebSocketServer({ port: 8080 });
    this.executionSessions = new Map();
  }

  async initialize() {
    try {
      // Connect MongoDB first
      await connectMongo()

      // Pull required Docker images first
      console.log('Pulling required Docker images...')
      await this.pullDockerImages()

      // Connect to RabbitMQ with connection options
      const amqpServer = 'amqp://guest:guest@localhost:5672'; // Adding default credentials
      this.connection = await amqp.connect(amqpServer, {
        heartbeat: 60,
        timeout: 60000
      });

      // Create channel with confirmation
      this.channel = await this.connection.createConfirmChannel();

      // Set prefetch
      await this.channel.prefetch(1);

      // Ensure queue exists with all options explicitly set
      const queue = 'CodeSender';
      const queueInfo = await this.channel.assertQueue(queue, {
        durable: true,
        autoDelete: false,
        exclusive: false
      });

      console.log(`Queue ${queueInfo.queue} is ready with ${queueInfo.messageCount} messages`);

      // Add robust error handling
      this.connection.on('error', (err) => {
        console.error('RabbitMQ connection error:', err);
        this.reconnect();
      });

      this.connection.on('close', () => {
        console.error('RabbitMQ connection closed. Attempting to reconnect...');
        this.reconnect();
      });

      // Start consuming with explicit options
      await this.channel.consume(queue, async (msg) => {
        if (msg !== null) {
          try {
            console.log('Received message:', msg.content.toString());
            await this.processMessage(msg);
            await this.channel.ack(msg);
            console.log('Message processed and acknowledged');
          } catch (error) {
            console.error('Error processing message:', error);
            // Reject the message and requeue it
            await this.channel.nack(msg, false, true);
          }
        } else {
          console.warn('Received null message from queue');
        }
      }, {
        noAck: false,
        exclusive: false
      });

      // Add WebSocket server
      this.wss.on('connection', (ws, req) => {
        const executionId = new URL(req.url, `ws://${req.headers.host}`).pathname.split('/')[1];
        console.log(`New connection for execution: ${executionId}`);

        // Store WebSocket connection
        this.executionSessions.set(executionId, ws);

        // Handle client disconnection
        ws.on('close', () => {
          console.log(`Connection closed for execution: ${executionId}`);
          this.executionSessions.delete(executionId);
        });
      });

      console.log('Worker service initialized and ready to process messages');
    } catch (error) {
      console.error('Error initializing worker service:', error);
      setTimeout(() => this.initialize(), 5000);
      throw error;
    }
  }

  async pullDockerImages() {
    const requiredImages = [
      'node:latest',
      'gcc:latest'
    ];

    for (const image of requiredImages) {
      try {
        // Check if image already exists locally
        const existingImages = await this.docker.listImages({
          filters: { reference: [image] }
        });
        if (existingImages.length > 0) {
          console.log(`Image ${image} already exists locally, skipping pull.`);
          continue;
        }

        console.log(`Pulling ${image}...`);
        await new Promise((resolve, reject) => {
          this.docker.pull(image, (err, stream) => {
            if (err) return reject(err);

            this.docker.modem.followProgress(stream, (err, output) => {
              if (err) return reject(err);
              console.log(`Successfully pulled ${image}`);
              resolve(output);
            });
          });
        });
      } catch (error) {
        console.error(`Error pulling ${image}:`, error);
        throw error;
      }
    }
  }

  async reconnect() {
    try {
      if (this.channel) {
        await this.channel.close();
      }
      if (this.connection) {
        await this.connection.close();
      }
    } catch (error) {
      console.error('Error closing existing connections:', error);
    }

    // Wait 5 seconds before attempting to reconnect
    setTimeout(() => {
      console.log('Attempting to reconnect...');
      this.initialize().catch(err => {
        console.error('Failed to reconnect:', err);
      });
    }, 5000);
  }

  async processMessage(msg) {
    let data; // Declared outside try so catch block can access it for error update
    try {
      data = JSON.parse(msg.content.toString());
      console.log('Parsed message data:', data);

      const language = (data.Lang || data.language || '').toLowerCase();
      const code = data.code;
      const input = data.input;

      if (!language) {
        throw new Error('Language not specified in the message');
      }

      console.log(`Processing ${language} code execution...`);

      const executionId = data.executionId || uuidv4();
      console.log(`Execution ID: ${executionId}`);

      // Mark execution as running in MongoDB
      await Execution.findOneAndUpdate(
        { executionId },
        { status: 'running' }
      );
      console.log(`Execution ${executionId} marked as running`);

      const workDir = path.join(__dirname, 'temp', executionId);
      await fs.mkdir(workDir, { recursive: true });

      const result = await this.executeCode(code, language, input, workDir);

      // Mark execution as completed in MongoDB
      await Execution.findOneAndUpdate(
        { executionId },
        {
          status: 'completed',
          output: result.logs,
          exitCode: result.exitCode,
          completedAt: new Date()
        }
      );
      console.log(`Execution ${executionId} marked as completed`);

      // Clean up temp files
      await fs.rm(workDir, { recursive: true, force: true });

      // Send result back to queue
      await this.channel.sendToQueue(
        'execution_results_queue',
        Buffer.from(JSON.stringify(result)),
        { persistent: true }
      );
    } catch (error) {
      console.error('Error processing message:', error);

      // Mark execution as failed in MongoDB if we have an executionId
      if (data?.executionId) {
        await Execution.findOneAndUpdate(
          { executionId: data.executionId },
          {
            status: 'failed',
            error: error.message,
            completedAt: new Date()
          }
        );
        console.log(`Execution ${data.executionId} marked as failed`);
      }

      await this.channel.sendToQueue(
        'execution_results_queue',
        Buffer.from(JSON.stringify({ error: error.message })),
        { persistent: true }
      );
    }
  }

  // Helper: wait for WebSocket connection for a given executionId
  waitForConnection(executionId, timeoutMs = 5000) {
    return new Promise((resolve) => {
      const existing = this.executionSessions.get(executionId);
      if (existing) return resolve(existing);

      const checkInterval = setInterval(() => {
        const ws = this.executionSessions.get(executionId);
        if (ws) {
          clearInterval(checkInterval);
          clearTimeout(timeout);
          resolve(ws);
        }
      }, 100);

      const timeout = setTimeout(() => {
        clearInterval(checkInterval);
        console.log(`WebSocket connection timeout for ${executionId}, proceeding without it`);
        resolve(null);
      }, timeoutMs);
    });
  }

  // Helper: get current ws for an execution (always fresh lookup)
  getWs(executionId) {
    return this.executionSessions.get(executionId);
  }

  async executeCode(code, language, input, workDir) {
    const containerConfig = this.getContainerConfig(language);
    const { imageName, command, extension } = containerConfig;
    const executionId = path.basename(workDir);
    let outputBuffer = [];

    try {
      // Write code to file in the working directory
      const filename = `code${extension}`;
      await fs.writeFile(path.join(workDir, filename), code);

      // Wait for WebSocket connection from the frontend (up to 5s)
      console.log(`Waiting for WebSocket connection for ${executionId}...`);
      const ws = await this.waitForConnection(executionId);
      if (ws) {
        console.log(`WebSocket connected for ${executionId}`);
      }

      const container = await this.docker.createContainer({
        Image: imageName,
        WorkingDir: '/code',
        Cmd: command(filename),
        AttachStdout: true,
        AttachStderr: true,
        AttachStdin: true,
        OpenStdin: true,
        StdinOnce: false,
        Tty: false,
        HostConfig: {
          Binds: [`${workDir}:/code`],
          Memory: 512 * 1024 * 1024,
          MemorySwap: 512 * 1024 * 1024,
          CpuPeriod: 100000,
          CpuQuota: 50000
        }
      });

      // Attach BEFORE starting so we capture all output from the very beginning
      const stream = await container.attach({
        stream: true,
        stdin: true,
        stdout: true,
        stderr: true
      });

      // Demux Docker's multiplexed stream (Tty:false adds 8-byte binary headers)
      const stdout = new PassThrough();
      const stderr = new PassThrough();
      this.docker.modem.demuxStream(stream, stdout, stderr);

      // Handler for clean output from stdout/stderr
      const handleOutput = (chunk, isError = false) => {
        const output = chunk.toString('utf8').replace(/\x1b\[[0-9;]*[a-zA-Z]/g, '');
        if (!output.trim()) return; // skip empty chunks
        outputBuffer.push(output);
        console.log(`[${executionId}] ${isError ? 'Stderr' : 'Output'}: ${output.trim()}`);

        const currentWs = this.getWs(executionId);
        if (currentWs?.readyState === WebSocket.OPEN) {
          currentWs.send(JSON.stringify({
            type: 'output',
            data: output,
            timestamp: Date.now()
          }));
        }
      };

      stdout.on('data', (chunk) => handleOutput(chunk, false));
      stderr.on('data', (chunk) => handleOutput(chunk, true));

      // Forward WebSocket input to container stdin
      const currentWs = this.getWs(executionId);
      if (currentWs) {
        currentWs.on('message', (msg) => {
          try {
            const parsed = JSON.parse(msg.toString());
            if (parsed.type === 'input' && parsed.data !== undefined) {
              stream.write(parsed.data + '\n');
              console.log(`[${executionId}] Stdin: ${parsed.data}`);
            }
          } catch (e) {
            // Raw string input
            stream.write(msg.toString() + '\n');
          }
        });
      }

      // Now start the container (output handler is already attached)
      await container.start();

      // Send initial status
      const statusWs = this.getWs(executionId);
      this.sendMessage(statusWs, 'status', 'Execution started');

      return new Promise((resolve) => {
        container.wait(async (err, data) => {
          const fullLogs = outputBuffer.join('\n');
          const doneWs = this.getWs(executionId);

          if (doneWs?.readyState === WebSocket.OPEN) {
            doneWs.send(JSON.stringify({
              type: 'logs',
              data: fullLogs,
              exitCode: data.StatusCode,
              timestamp: Date.now()
            }));

            // Send completion
            this.sendMessage(doneWs, 'completion', 'Execution finished', data.StatusCode);
          }

          // Get Docker engine logs
          const dockerLogs = await container.logs({
            stdout: true,
            stderr: true,
            timestamps: true
          });

          // Save logs to file
          await fs.writeFile(
            path.join(workDir, 'docker.log'),
            dockerLogs.toString('utf8')
          );

          await container.remove();
          resolve({
            exitCode: data.StatusCode,
            logs: fullLogs,
            dockerLogs: dockerLogs.toString('utf8')
          });
        });
      });
    } catch (error) {
      const errWs = this.getWs(executionId);
      this.sendMessage(errWs, 'error', error.message);
      throw error;
    }
  }

  getContainerConfig(language) {
    const configs = {
      javascript: {
        imageName: 'node:latest',
        extension: '.js',
        command: (filename) => ['node', filename]
      },
      cpp: {
        imageName: 'gcc:latest',
        extension: '.cpp',
        command: (filename) => ['sh', '-c', `g++ ${filename} -o output && ./output`]
      }
    };

    return configs[language.toLowerCase()] || configs.javascript;
  }

  sendMessage(ws, type, data, exitCode = null) {
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type,
        data,
        ...(exitCode !== null && { exitCode }),
        timestamp: Date.now()
      }));
    }
  }

  async shutdown() {
    try {
      if (this.channel) await this.channel.close();
      if (this.connection) await this.connection.close();
    } catch (error) {
      console.error('Error shutting down worker service:', error);
    }
  }
}
