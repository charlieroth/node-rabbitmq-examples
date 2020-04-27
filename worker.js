const amqp = require("amqplib");
const NR = require("node-resque");
const IORedis = require("ioredis");
const { v4 } = require("uuid");

const redis = new IORedis();

// rabbitmq queue names
const smallTasksQueue = "small-tasks";
const mediumTaskQueue = "medium-tasks";
const largeTaskQueue = "large-tasks";

// node-resque job/queue names
const jobNames = {
    sendResponse: "sendResponse",
    uploadData: "uploadData",
    requestStatus: "requestStatus"
};
const queueNames = {
    responseQueue: "responseQueue",
    uploadQueue: "uploadQueue",
    requestStatusQueue: "requestStatusQueue"
};
const requestStatuses = {
    pending: "0",
    success: "1",
    failure: "-1"
};

const jobs = {
    [jobNames.sendResponse]: {
        perform: async (uploadId) => {
            const result = await new Promise((resolve) => {
                // await redis.del(`worker-queue:${uploadId}`);
                setTimeout(() => {
                    console.log("Response sent for uploadId: %s !!", uploadId);
                    resolve(true);
                }, 300);
            });

            return result;
        }
    },
    [jobNames.uploadData]: {
        perform: async (uploadId, numJobs) => {
            const result = await new Promise((resolve) => {
                for (let i = 0; i < numJobs; i++) {
                    // Random amount of time (ms) to retrieve requestId
                    let requestLength = 100 * (Math.floor(Math.random() * 3) + 1);
                    setTimeout(async () => {
                        const requestId = v4();
                        await redis.hset(`worker-queue:${uploadId}`, requestId, requestStatuses.pending);
                        await queue.enqueue(
                            queueNames.requestStatusQueue,
                            jobNames.requestStatus,
                            [uploadId, requestId]
                        );
                    }, requestLength);
                }
                resolve(true);
            });

            return result;
        }
    },
    [jobNames.requestStatus]: {
        plugins: ["Retry"],
        pluginOptions: {
            Retry: {
                retryLimit: 3,
                retryDelay: 3000
            }
        },
        perform: async (uploadId, requestId) => {
            try {
                console.log("Polling for request id: ", requestId);
                // Random amount of time (ms) to check status of requestId
                let requestLength = 100 * (Math.floor(Math.random() * 4) + 1);
                // Random number to indicate whether the api call was successful or needs to be retried
                let apiHit = Math.floor(Math.random() * 100) + 1;
                const requestProcessed = await new Promise((resolve) => {
                    setTimeout(() => {
                        const success = apiHit > 40;
                        resolve(success);
                    }, requestLength);
                });

                if (requestProcessed) {
                    await redis.hset(`worker-queue:${uploadId}`, requestId, requestStatuses.success);
                    return requestProcessed;
                } else {
                    throw new Error(`Still processing request id: ${requestId}`);
                }
            } catch (e) {
                console.log(e.message);
                throw e;
            }
        }
    }
}

const queue = new NR.Queue({connection: { redis }}, jobs);
const scheduler = new NR.Scheduler(
    { 
        connection: { redis }, 
        queues: [
            queueNames.uploadQueue,
            queueNames.requestStatusQueue,
            queueNames.responseQueue
        ]
    }, 
    jobs
);
const multiWorker = new NR.MultiWorker(
    {
        connection: { redis },
        queues: [queueNames.uploadQueue, queueNames.requestStatusQueue, queueNames.responseQueue],
        minTaskProcessors: 1,
        maxTaskProcessors: 5 
    },
    jobs
);

async function checkUploadCompletion(uploadId) {
    console.log("Checking status of upload: ", uploadId);
    const workerQueueHash = await redis.hgetall(`worker-queue:${uploadId}`);
    const queueValues = Object.values(workerQueueHash);
    if (queueValues.includes("0")) {
        console.log(`INCOMPLETE uploadId: ${uploadId}`);
    } else {
        console.log(`COMPLETE uploadId: ${uploadId}`);
        await queue.enqueue(queueNames.responseQueue, jobNames.sendResponse, uploadId);
    }
}
    
multiWorker.on("reEnqueue", async (workerId, q, job, plugin) => {
    console.log(`
        event: reEnqueue
        worker[${workerId}]
        queue: ${q}
        job.args: ${JSON.stringify(job.args)}
        plugin: ${JSON.stringify(plugin)}
    `);
});

multiWorker.on("failure", async (workerId, q, job, _failure, duration) => {
    console.log(`
        event: failure
        worker[${workerId}]
        queue: ${q}
        job.args: ${JSON.stringify(job.args)}
        duration: (${duration}ms)
    `);
    if (q === queueNames.requestStatusQueue) {
        console(`Failed to process requestId: ${job.args[1]}`);
        redis.hset(`worker-queue:${job.args[0]}`, `${job.args[1]}`, requestStatuses.failure).then(async () => {
            await checkUploadCompletion(job.args[0]);
        });
    }
});

multiWorker.on("success", async (workerId, q, job, result, duration) => {
    console.log(`
        event: success
        worker[${workerId}]
        queue: ${q}
        job.args: ${JSON.stringify(job.args)}
        result: ${result} 
        duration: (${duration}ms)
    `);
    if (q === queueNames.requestStatusQueue) {
        redis.hset(`worker-queue:${job.args[0]}`, `${job.args[1]}`, requestStatuses.success).then(async () => {
            await checkUploadCompletion(job.args[0]);
        });
    }
});

async function startConsumer(channel, q) {
    await channel.assertQueue(q, { durable: true });
    console.log("[*] Waiting for messages in %s. To exit press Ctrl+c", q);
    channel.consume(q, async (msg) => {
        console.log("[%s] Recieved %s", q, msg.content.toString());
        const message = JSON.parse(msg.content.toString());
        await queue.enqueue(
            queueNames.uploadQueue,
            jobNames.uploadData,
            [message.uploadId, message.numJobs]
        );
    }, { noAck: true });
}

async function main() {
    try {
        multiWorker.start();
        await scheduler.connect();
        scheduler.start();
        await queue.connect();
        const connection = await amqp.connect("amqp://localhost");
        const channel = await connection.createChannel();
        startConsumer(channel, smallTasksQueue);
        startConsumer(channel, mediumTaskQueue);
        startConsumer(channel, largeTaskQueue);
    } catch (e) {
        console.error(e);
        shutdown().finally(() => process.exit(0));
    }
}

async function shutdown() {
    await scheduler.end();
    multiWorker.end();
    const sts = await redis.quit();
    if (sts === "OK") console.log("ioredis client successfully quit");
    else console.log("ioredis client failed to quit");
}

process.on("message", (msg) => {
    if (msg == "shutdown") {
        console.log('Closing all connections...');
        shutdown().finally(() => process.exit(0));
    }
});

process.on("SIGINT", () => {
    console.log("SIGINT triggered");
    shutdown().finally(() => process.exit(0));
});

process.on("SIGTERM", () => {
    console.log("SIGTERM triggered");
    shutdown().finally(() => process.exit(1));
});

process.on("uncaughtException", (err) => {
    console.log(`Uncaught Exception: ${err.message}`);
    process.exit(1);
});

process.on("unhandledRejection", (reason, promise) => {
    console.log("Uncaught Rejection at: ", promise, ` reason: ${reason}`);
    process.exit(1);
});

process.on("exit", (code) => {
    console.log("Worker exited with code: ", code);
});

main();

if (typeof process.send === "function") {
    process.send("ready");
}
