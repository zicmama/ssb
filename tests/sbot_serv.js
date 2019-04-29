const { readFileSync } = require('fs')
const { generate } = require('ssb-keys')
const pull = require('pull-stream')
const tape = require('tape')
const parallel = require('run-parallel')

const createSbot = require('ssb-server')
  .use(require('ssb-server/plugins/logging'))
  .use(require('ssb-gossip'))
  .use(require('ssb-replicate'))
  .use(require('ssb-private'))
  .use(require('ssb-friends'))
  .use(require('ssb-blobs'))
  .use(require('ssb-ebt'))


const testName = process.env['TEST_NAME']
const testBob = process.env['TEST_BOB']
const testPort = process.env['TEST_PORT']

const scriptBefore = readFileSync(process.env['TEST_BEFORE']).toString()
const scriptAfter = readFileSync(process.env['TEST_AFTER']).toString()

tape.createStream().pipe(process.stderr);
tape(testName, function (t) {
//   t.timeoutAfter(30000) // doesn't exit the process
//   const tapeTimeout = setTimeout(() => {
//     t.comment("test timeout")
//     process.exit(1)
//   }, 50000)
  
  

  function exit() { // call this when you're done
    sbot.close()
    t.comment('closed jsbot')
    // clearTimeout(tapeTimeout)
    t.end()
  }

  const sbot = createSbot({
    port: testPort,
    temp: testName,
    keys: generate(),
    replicate: {"legacy":false},
  })
  const alice = sbot.whoami()

//   const replicate_changes = sbot.replicate.changes()

  t.comment("sbot spawned, running before")
  eval(scriptBefore)
  
  function ready() {
    console.log(alice.id) // tell go process who our pubkey
  }
  
  
  sbot.on("rpc:connect", (remote, isClient) => {
    t.equal(testBob, remote.id, "correct ID")
    // t.true(isClient, "connected remote is client")
    t.comment(JSON.stringify(remote))
    eval(scriptAfter)
    // return true
  })
})
