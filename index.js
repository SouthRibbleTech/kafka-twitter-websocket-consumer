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
        this.isAlive = false
    }

    noop(params) {}

    listen() {
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

        this.wss.on('connection', async (ws)=>{
            ws.on('message', (message)=>{
                console.log(`received: ${message}`)
                ws.send(`We got your message: ${message}`)
            })
            
            ws.send('You are connected to the server')
            let alive = true
            

            setInterval(()=>{
                if(this.isAlive === false) {
                    try{
                        this.consumer.disconnect()
                    }catch(err){
                        console.log("Error disconnecting from kafka: ", err)
                    }
                    try{
                        ws.terminate()
                    }catch(err){
                        console.log("Error disconnecting web socket client: ", err)
                    }
                }else{
                    ws.ping(this.noop)
                }
            }, 30000)
        
            await this.consumer.connect()
            await this.consumer.subscribe({ topic: 'tweets', fromBeginning: false })
            await this.consumer.run({
                eachMessage: async ({ topic, partition, message}) => {
                    ws.send(message.value.toString())
                }
            })

            ws.on('pong',()=>{
                this.isAlive = true
            })
        })
    }

    
}

var tw = new TwitterWS()
tw.listen()