#!/usr/bin/env node

const amqp = require("amqplib");
const { v4 } = require("uuid");

async function main() {
    const queue = "mc-tasks";
    try {
        const connection = await amqp.connect("amqp://localhost"); 
        const channel = await connection.createChannel();
        await channel.assertQueue(queue, { durable: true });
        
        for (let i = 0; i < 4; i++) {
            let randomWait = 1000 * (Math.floor(Math.random() * 4) + 1);
            let numJobs = Math.floor(Math.random() * 3) + 1;
            setTimeout(() => {
                const msg = JSON.stringify({
                    uploadId: v4(),
                    numJobs
                });
                channel.sendToQueue(
                    queue, 
                    Buffer.from(msg), 
                    { persistent: true }
                );
                console.log("[%s] Sent: %s", i, msg);
            }, randomWait);
        }
    } catch (e) {
        console.error(e);
        process.exit(0);
    }
}

main();
