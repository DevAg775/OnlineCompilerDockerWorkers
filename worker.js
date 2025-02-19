import amqp from 'amqplib';
import Docker from 'dockerode';
import { promises as fs } from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import WebSocket, { WebSocketServer } from 'ws';
import { fileURLToPath } from 'url';

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
      // Pull required Docker images first
      console.log('Pulling required Docker images...');
      await this.pullDockerImages();

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
    try {
      const data = JSON.parse(msg.content.toString());
      console.log('Parsed message data:', data); // Debug log
      
      // Get language from either 'Lang' or 'language' field
      const language = (data.Lang || data.language || '').toLowerCase();
      const code = data.code;
      const input = data.input;

      if (!language) {
        throw new Error('Language not specified in the message');
      }

      console.log(`Processing ${language} code execution...`); // Debug log

      // Create unique working directory for this execution
      const executionId = uuidv4();
      const workDir = path.join(__dirname, 'temp', executionId);
      await fs.mkdir(workDir, { recursive: true });

      // Execute code based on language
      const result = await this.executeCode(code, language, input, workDir);

      // Clean up
      await fs.rmdir(workDir, { recursive: true });

      // Send result back
      await this.channel.sendToQueue(
        'execution_results_queue',
        Buffer.from(JSON.stringify(result)),
        { persistent: true }
      );
    } catch (error) {
      console.error('Error processing message:', error);
      await this.channel.sendToQueue(
        'execution_results_queue',
        Buffer.from(JSON.stringify({ error: error.message })),
        { persistent: true }
      );
    }
  }

  async executeCode(code, language, input, workDir) {
    const containerConfig = this.getContainerConfig(language);
    const { imageName, command, extension } = containerConfig;
    const executionId = path.basename(workDir);
    const ws = this.executionSessions.get(executionId);
    let outputBuffer = [];

    try {
      const container = await this.docker.createContainer({
        Image: imageName,
        WorkingDir: '/code',
        Cmd: command(extension),
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

      await container.start();
      const stream = await container.attach({
        stream: true,
        stdin: true,
        stdout: true,
        stderr: true
      });

      if (ws) {
        // Collect and send real-time logs
        stream.on('data', (chunk) => {
          const output = chunk.toString('utf8').replace(/\x1b\[[0-9;]*[a-zA-Z]/g, '');
          outputBuffer.push(output);
          
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
              type: 'output',
              data: output,
              timestamp: Date.now()
            }));
          }
        });

        // Send initial status
        this.sendMessage(ws, 'status', 'Execution started');
      }

      return new Promise((resolve) => {
        container.wait(async (err, data) => {
          if (ws?.readyState === WebSocket.OPEN) {
            // Send final logs
            const fullLogs = outputBuffer.join('\n');
            ws.send(JSON.stringify({
              type: 'logs',
              data: fullLogs,
              exitCode: data.StatusCode,
              timestamp: Date.now()
            }));

            // Send completion
            this.sendMessage(ws, 'completion', 'Execution finished', data.StatusCode);
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
      this.sendMessage(ws, 'error', error.message);
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
        command: (filename) => ['g++', filename, '-o', 'output', '&&', './output']
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
