const { Kafka } = require('kafkajs')
const WebSocket = require('ws')

console.log("Connecting to kafka server...")
const kafka = new Kafka({
    clientId: 'mysql_twitter',
    brokers: ['192.168.0.6:9092']
  })
  const consumer = kafka.consumer({ groupId: 'websocket_twitter' })

const wss = new WebSocket.Server({
    port: 9090,
    perMessageDeflate: {
        zlibDeflateOptions: {
            chunkSize: 1024,
            memLevel: 7,
            level: 3
        },
        zlibInflateOptions: {
            chunkSize: 10 * 1024
        },
        clientNoContextTakeover: true,
        serverNoContextTakeover: true,
        serverMaxWindowBits: 10,
        concurrencyLimit: 10,
        threshold: 1024
    }
})

wss.on('connection', async (ws)=>{
    ws.on('message', (message)=>{
        console.log(`received: ${message}`)
        ws.send(`We got your message: ${message}`)
    })
    
    ws.send('You are connected to the server')

    await consumer.connect()
    await consumer.subscribe({ topic: 'tweets', fromBeginning: false })
    await consumer.run({
        eachMessage: async ({ topic, partition, message}) => {
            ws.send(message.value.toString())
        }
    })

})