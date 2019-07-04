const assert = require("assert")

const apiKey = "b-TM2X4eQwOpeWV85QQdQAKh6dOJgFRwiVfsi1VwkJkw"
const streamName = "streamr-client-resend-bug-" + Date.now()

const StreamrClient = require("streamr-client")

const startTime = Date.now()
function log(...args) {
    console.log(Date.now() - startTime, ...args)
}

async function run() {
    const sendQueue = [
        { topic: "test", value: 1 },
        { topic: "test", value: 2 },
        { topic: "test", value: 3 },
    ]
    log(`Sending ${sendQueue.length} data points...`)
    await send(sendQueue)

    // REPRO: try different sleep values here
    log("Sleeping...")
    await sleep(0)
    //await sleep(1000)

    const recvQueue = []
    log("Receiving data...")
    await receive(recvQueue)

    log("Sleeping...")
    await sleep(10000)
    assert.deepStrictEqual(sendQueue, recvQueue)
}

async function send(messageArray) {
    const client = new StreamrClient({
        auth: {
            privateKey: '0x1234576812345768123457681234576812345768123457681234576812345768'
        }
    })
    const sendStream = await client.getOrCreateStream({
        name: streamName,
        public: true,
    })

    for (const message of messageArray) {
        await sendStream.publish(message)
        await sleep(0)  // not sleeping here messed serial publish for me in another project, couldn't repro though
    }
    await client.disconnect()
}

async function receive(bufferArray) {
    const client = new StreamrClient({
        auth: {
            privateKey: '0x1234576812345768123457681234576812345768123457681234576812345768'
        }
    })
    client.on("connected", function() { log("A connection has been established!") })
    const recvStream = await client.getStreamByName(streamName)

    const sub = client.subscribe({
        stream: recvStream.id,
        resend: {
            from: {
                timestamp: 0,
            },
        },
    }, (msg, meta) => {
        log(JSON.stringify(meta))
        bufferArray.push(msg)
    })
    sub.on("error", function (error) { log("ERROR: " + JSON.stringify(error)) })
    sub.on("resending", () => { log("RESENDING") })
    sub.on("resent", () => { log("RESENT") })
    sub.on("no_resend", () => { log("NO_RESEND") })
}

function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms)
    })
}

run().catch(e => {
    console.error(e.stack)
}).then(async () => {
    await sleep(1000)
    process.exit(0)
})
