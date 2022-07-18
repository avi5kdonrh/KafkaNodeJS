const fs = require('fs');
const { Kafka, Partitioners } = require('kafkajs');

const sender = async () => {
const kafkaClient =  new Kafka({
    brokers: ['tls-route:443'],
    ssl: {
      ca: fs.readFileSync('/home/ca_crt/location/ca.crt', {encoding:'utf8', flag:'r'})
    },
    sasl: {
        mechanism: 'scram-sha-512',
        username: 'scram-user',
        password: 'password'
    }
})

const producer = kafkaClient.producer({
    createPartitioner: Partitioners.LegacyPartitioner
});
await producer.connect();
await producer.send({
    topic: 'my-topic',
    messages: [{value:new Date().getTime().toString()}]
})

await producer.disconnect()

}

sender().catch( err => {
    console.log(err);
    process.exit(1);
})
