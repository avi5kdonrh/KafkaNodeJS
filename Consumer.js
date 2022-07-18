const fs = require('fs');
const { Kafka } = require('kafkajs');

const receiver = async () => {
const kafkaClient =  new Kafka({
    brokers: ['tls-route:443'],
    ssl: {
      ca: fs.readFileSync('/path/to/ca.crt', {encoding:'utf8', flag:'r'})
    },
    sasl: {
        mechanism: 'scram-sha-512',
        username: 'scram-user',
        password: 'password'
    }
})

const consumer = kafkaClient.consumer({
    groupId: 'my-group'
})

await consumer.connect();
await consumer.subscribe({
    topic: 'my-topic'
})

await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        console.log({
            key: message.key === null?null:message.key.toString(),
            value: message.value.toString(),
            partition: partition
        })
    }
})
}

receiver().catch( err => {
    console.log(err);
    process.exit(1)
})
