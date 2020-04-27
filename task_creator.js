const amqp = require("amqplib");
const { v4 } = require("uuid");

async function main() {
    const smallTasksQueue = "small-tasks";
    const mediumTaskQueue = "medium-tasks";
    const largeTaskQueue = "large-tasks";

    try {
        const connection = await amqp.connect("amqp://localhost"); 
        const channel = await connection.createChannel();
        await channel.assertQueue(smallTasksQueue, { durable: true });
        await channel.assertQueue(mediumTaskQueue, { durable: true });
        await channel.assertQueue(largeTaskQueue, { durable: true });
        
        for (let i = 1; i <= 1; i++) {
            let randomWait = 1000 * (Math.floor(Math.random() * 4) + 1);
            let numJobs = Math.floor(Math.random() * 10) + 1;
            setTimeout(() => {
                const msg = JSON.stringify({
                    uploadId: v4(),
                    numJobs
                });
                if (numJobs <= 3) {
                    channel.sendToQueue(
                        smallTasksQueue, 
                        Buffer.from(msg), 
                        { persistent: true }
                    );
                } else if (numJobs > 3 && numJobs <= 7) {
                    channel.sendToQueue(
                        mediumTaskQueue, 
                        Buffer.from(msg), 
                        { persistent: true }
                    );
                } else if (numJobs > 7) {
                    channel.sendToQueue(
                        largeTaskQueue, 
                        Buffer.from(msg), 
                        { persistent: true }
                    );
                } else {
                    console.log("unable to send jobs");
                }
                console.log("[%s] Sent: %s", i, msg);
            }, randomWait);
        }
    } catch (e) {
        console.error(e);
        process.exit(0);
    }
}

main();
