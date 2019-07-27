const { Kafka } = require('kafkajs')
const WebSocket = require('ws')

class TwitterWS {

    constructor() {
        console.log("Connecting to kafka server...")
        const kafka = new Kafka({
            clientId: 'mysql_twitter',
            brokers: ['192.168.0.6:9092']
        })
        this.consumer = kafka.consumer({ groupId: 'websocket_twitter' })
        this.wss = null
    }

    
    async listen() {
        try{
            this.wss = new WebSocket.Server({
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
            console.log("Web socket server listening on port: 9090")
        }catch(err){
            console.log("Error starting web socket server:", err)
        }
        this.wss.on('connection', (ws)=>{
            ws.send('You are connected to the server')
        })

        await this.consumer.connect()
        await this.consumer.subscribe({ topic: 'tweets', fromBeginning: false })
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message}) => {
                this.wss.clients.forEach((ws)=>{
                    ws.send(message.value.toString())
                })
            }
        })
    }
}

var tw = new TwitterWS()
tw.listen()