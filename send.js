#!/usr/bin/env node

const amqp = require("amqplib");

async function main() {
    try {
        const connection = await amqp.connect("amqp://localhost"); 
        const channel = await connection.createChannel();
        await channel.assertQueue("hello", { durable: false });
        channel.sendToQueue("hello", Buffer.from("Hello, 1"));
        channel.sendToQueue("hello", Buffer.from("Hello, 2"));

        setTimeout(() => {
            channel.sendToQueue("hello", Buffer.from("Hello, 3"));
            channel.sendToQueue("hello", Buffer.from("Hello, 4"));
            channel.sendToQueue("hello", Buffer.from("Hello, 5"));
        }, 3000);
    } catch (e) {
        console.error(e);
        process.exit(0);
    }
}

main();
