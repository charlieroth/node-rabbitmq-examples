#!/usr/bin/env node

const amqp = require("amqplib");

async function main() {
    let channel;
    try {
        const connection = await amqp.connect("amqp://localhost");
        channel = await connection.createChannel();
        await channel.assertQueue("hello", { durable: false });
    } catch (e) {
        console.error(e);
        process.exit(0);
    }
    
    console.log("[*] Waiting for messages in  %s. To exit press Ctrl+c", "hello");
    channel.consume("hello", async (msg) => {
        console.log("[x] Recieved %s", msg.content.toString());
    }, { noAck: true });
}

main();
