package tests

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/netwrap"
	"go.cryptoscope.co/secretstream"
	"go.cryptoscope.co/ssb/multilogs"
)

const createSomeMsgs = `
function mkMsg(msg) {
	return function(cb) {
		sbot.publish(msg, cb)
	}
}
n = 25
let msgs = []
for (var i = n; i>0; i--) {
	msgs.push(mkMsg({type:"test", text:"foo", i:i}))
}
msgs.push(mkMsg({type:"contact", "contact": testBob, "following": true}))
parallel(msgs, function(err, results) {
	t.error(err, "parallel of publish")
	t.equal(n+1, results.length, "message count")
	ready() // triggers connect and after block
})
`

func TestEpidemic(t *testing.T) {
	r := require.New(t)

	sbot, alice, port, done, errc := initInteropAsClient(t,
		createSomeMsgs,
		`t.comment("ohai");setTimeout(exit,10000)`)

	wrappedAddr := netwrap.WrapAddr(&net.TCPAddr{
		IP:   net.ParseIP("192.168.42.101"),
		Port: port,
	}, secretstream.Addr{PubKey: alice.ID})

	publish, err := multilogs.OpenPublishLog(sbot.RootLog, sbot.UserFeeds, *sbot.KeyPair)
	r.NoError(err)

	var tmsgs = []interface{}{
		map[string]interface{}{
			"type":  "about",
			"about": sbot.KeyPair.Id.Ref(),
			"name":  "test user",
		},
		map[string]interface{}{
			"type":      "contact",
			"contact":   alice.Ref(),
			"following": true,
		},
		map[string]interface{}{
			"type": "text",
			"text": `# hello world!`,
		},
		map[string]interface{}{
			"type":  "about",
			"about": alice.Ref(),
			"name":  "test alice",
		},
	}
	for i, msg := range tmsgs {
		newSeq, err := publish.Append(msg)
		r.NoError(err, "failed to publish test message %d", i)
		r.NotNil(newSeq)
	}

	g, err := sbot.GraphBuilder.Build()
	t.Log("build:", err)
	err = g.RenderSVGToFile(fmt.Sprintf("%s.svg", t.Name()))
	t.Log("render:", err)

	err = sbot.Node.Connect(context.TODO(), wrappedAddr)
	r.NoError(err, "connect failed")

	<-done

	sbot.Shutdown()
	r.NoError(sbot.Close())
	for err := range mergeErrorChans(errc) {
		t.Error(err)
	}

}
