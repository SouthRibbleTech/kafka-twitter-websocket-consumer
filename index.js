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
        this.keyword_count = {
            boris: 0,
            trump: 0,
            iran: 0
        }
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
        await this.consumer.subscribe({ topic: 'tweets', fromBeginning: true })
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message}) => {
                    var textTxt = message.value.text.toString().toLowerCase()
                    var extendedTxt = message.value.extended_tweet.full_text.toString().toLowerCase()
                    console.log(textTxt)
                    console.log("Extended: ", extendedTxt)
                    if(msgTxt.indexOf('trump') || extendedTxt.indexOf('trump')){
                        this.keyword_count.trump ++
                    }
                    if(message.value.text.toLowerCase().indexOf('boris')){
                        this.keyword_count.boris ++
                    }
                    if(message.value.text.toLowerCase().indexOf('iran')){
                        this.keyword_count.iran ++
                    }
                    if(message.value.text.toLowerCase().indexOf('bitcoin')){
                        this.keyword_count.bitcoin ++
                    }
            }
        })

        setInterval(()=>{
            this.wss.clients.forEach((ws)=>{
                ws.send(JSON.stringify(this.keyword_count))
            })
        },5000)
    }
}

var tw = new TwitterWS()
tw.listen()