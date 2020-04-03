#!/usr/bin/env node

const amqp = require("amqplib");

async function main() {
    try {
        const connection = await amqp.connect("amqp://localhost"); 
        const channel = await connection.createChannel();
        await channel.assertQueue("hello", { durable: false });
        channel.sendToQueue("hello", Buffer.from("Hello, Charlie"));
        channel.sendToQueue("hello", Buffer.from("Hello, Miranda"));

        setTimeout(() => {
            channel.sendToQueue("hello", Buffer.from("Hello, Parker"));
            channel.sendToQueue("hello", Buffer.from("Hello, Bootle"));
            channel.sendToQueue("hello", Buffer.from("Hello, Random"));
        }, 3000);
    } catch (e) {
        console.error(e);
        process.exit(0);
    }
}

main();
