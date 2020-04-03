#!/usr/bin/env node

const amqp = require("amqplib");
const NR = require("node-resque");
const IORedis = require("ioredis");
const { v4 } = require("uuid");

const redis = new IORedis();

const jobs = {
    notifyUser: {
        perform: async (uploadId) => {
            const result = await new Promise((resolve) => {
                setTimeout(() => {
                    console.log("User notified for uploadId: ", uploadId);
                    resolve(true);
                }, 300);
            });

            return result;
        }
    },
    getRequestIds: {
        perform: async (uploadId, numJobs) => {
            const result = await new Promise((resolve) => {
                for (let i = 0; i < numJobs; i++) {
                    let requestLength = 100 * (Math.floor(Math.random() * 4) + 1);
                    setTimeout(async () => {
                        const requestId = v4();
                        //console.log(
                            //"Upload ID:  %s, Request ID: %s", 
                            //uploadId, 
                            //requestId
                        //);
                        await redis.hset(`mc-worker-queue:${uploadId}`, requestId, 0);
                        await queue.enqueue(
                            "get-request-status", 
                            "getRequestIdStatus",
                            [uploadId, requestId]
                        );
                    }, requestLength);
                }
                resolve(true);
            });

            return result;
        }
    },
    getRequestIdStatus: {
        plugins: ["Retry"],
        pluginOptions: {
            Retry: {
                retryLimit: 3,
                retryDelay: 1000
            }
        },
        perform: async (uploadId, requestId) => {
            //console.log('---------------------------------------------------');
            try {
                //console.log(`
                    //Polling...
                    //requestId = ${requestId}
                    //uploadId = ${uploadId}
                //`);
                let requestLength = 100 * (Math.floor(Math.random() * 4) + 1);
                let apiHit = Math.floor(Math.random() * 100) + 1;
                const requestProcessed = await new Promise((resolve) => {
                    setTimeout(() => {
                        const success = apiHit > 40;
                        resolve(success);
                    }, requestLength);
                });

                //console.log(`Request ID: ${requestId}, requestProcessed: ${requestProcessed}`);
                if (requestProcessed) {
                    await redis.hset(`mc-worker-queue:${uploadId}`, requestId, 1);
                    return requestProcessed;
                } else {
                    throw new Error(`Request ID: ${requestId} still processing...`);

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
            "get-mc-request-id",
            "get-request-status",
            "send-response-email"
        ]
    }, 
    jobs
);
const multiWorker = new NR.MultiWorker(
    {
        connection: { redis },
        queues: ["get-mc-request-id", "get-request-status", "send-response-email"],
        minTaskProcessors: 1,
        maxTaskProcessors: 5 
    },
    jobs
);

async function checkUploadCompletion(uploadId) {
    const workerQueueHash = await redis.hgetall(`mc-worker-queue:${uploadId}`);
    console.log("workerQueueHash: ", workerQueueHash);
    const requestStatuses = Object.values(workerQueueHash);
    //console.log("requestStatuses: ", requestStatuses);
    let allComplete = true;
    for (const reqStat of requestStatuses) {
        if (reqStat === '0') {
            allComplete = false;
            break;
        }
    }

    if (allComplete) {
        await queue.enqueue("send-response-email", "notifyUser", uploadId);
    }

    return;
}

async function main() {
    multiWorker.on("reEnqueue", async (workerId, queue, job, plugin) => {
        //console.log("---------------------------------------------------");
        console.log(`
            worker[${workerId}]
            REENQUEUE: ${queue}
            job.args: ${JSON.stringify(job.args)}
            plugin: ${JSON.stringify(plugin)}
        `);
        //console.log("---------------------------------------------------");
    });

    multiWorker.on("failure", async (workerId, queue, job, _failure, duration) => {
        //console.log("---------------------------------------------------");
        console.log(`
            worker[${workerId}]
            job.args: ${JSON.stringify(job.args)}
            FAILURE: ${queue} >> Failed to process requestId: ${job.args[1]}
            (${duration}ms)
        `);
        if (queue === "get-request-status") {
            await redis.hset(`mc-worker-queue:${job.args[0]}`, `${job.args[1]}`, -1);
            await checkUploadCompletion(job.args[0]);
        }
        //console.log("---------------------------------------------------");
    });

    multiWorker.on("success", async (workerId, queue, job, result, duration) => {
        //console.log("---------------------------------------------------");
        console.log(`
            worker[${workerId}]
            job.args: ${JSON.stringify(job.args)}
            SUCCESS: ${queue} >> ${result} 
            (${duration}ms)
        `);
        if (queue === "get-request-status") {
            await redis.hset(`mc-worker-queue:${job.args[0]}`, `${job.args[1]}`, 1);
            await checkUploadCompletion(job.args[0]);
        }
        //console.log("---------------------------------------------------");
    });
    
    try {
        multiWorker.start();
        await scheduler.connect();
        scheduler.start();
        await queue.connect();
        const connection = await amqp.connect("amqp://localhost");
        const channel = await connection.createChannel();
        await channel.assertQueue("mc-tasks", { durable: true });
        console.log("[*] Waiting for messages in  %s. To exit press Ctrl+c", "hello");
        channel.consume("mc-tasks", async (msg) => {
            console.log("[x] Recieved %s", msg.content.toString());
            const message = JSON.parse(msg.content.toString());
            await queue.enqueue(
                "get-mc-request-id",
                "getRequestIds", 
                [message.uploadId, message.numJobs]
            );
        }, { noAck: true });
    } catch (e) {
        console.error(e);
        process.exit(0);
    }
}

process.on("SIGNINT", async () => {
    await multiWorker.end();
    await scheduler.end();
});

main();
