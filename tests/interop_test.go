package tests

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cryptix/go/logging"
	"github.com/cryptix/go/logging/logtest"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/muxrpc/debug"
	"go.cryptoscope.co/netwrap"
	"go.cryptoscope.co/ssb"
	"go.cryptoscope.co/ssb/sbot"
)

func init() {
	err := os.RemoveAll("testrun")
	if err != nil {
		fmt.Println("failed to clean testrun dir")
		panic(err)
	}
}

func writeFile(t *testing.T, data string) string {
	r := require.New(t)
	f, err := ioutil.TempFile("testrun/"+t.Name(), "*.js")
	r.NoError(err)
	_, err = fmt.Fprintf(f, "%s", data)
	r.NoError(err)
	err = f.Close()
	r.NoError(err)
	return f.Name()
}

func initSbot(t *testing.T, sbotOpts ...sbot.Option) (*sbot.Sbot, <-chan error) {
	r := require.New(t)

	dir := filepath.Join("testrun", t.Name())
	os.RemoveAll(dir)
	// Choose you logger!
	// use the "logtest" line if you want to log through calls to `t.Log`
	// use the "NewLogfmtLogger" line if you want to log to stdout
	// the test logger does not print anything if the command hangs, so you have an alternative
	var info logging.Interface
	if testing.Verbose() {
		// TODO: multiwriter
		info = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	} else {
		info, _ = logtest.KitLogger("go", t)
	}
	// timestamps!
	info = log.With(info, "ts", log.TimestampFormat(time.Now, "3:04:05.000"))

	// prepend defaults
	sbotOpts = append([]sbot.Option{
		sbot.WithInfo(info),
		sbot.WithListenAddr("localhost:0"),
		sbot.WithRepoPath(dir),
		sbot.WithConnWrapper(func(conn net.Conn) (net.Conn, error) {
			if !testing.Verbose() {
				return conn, nil
			}
			t.Log("wrapping:", conn.RemoteAddr().String())
			return debug.WrapConn(info, conn), nil
		}),
	}, sbotOpts...)

	sbot, err := sbot.New(sbotOpts...)
	r.NoError(err, "failed to init test go-sbot")
	t.Logf("go-sbot: %s", sbot.KeyPair.Id.Ref())

	var errc = make(chan error, 1)
	go func() {
		err := sbot.Node.Serve(context.TODO())
		if err != nil {
			errc <- errors.Wrap(err, "node serve exited")
		}
		close(errc)
	}()
	return sbot, errc
}

// returns the created go-sbot, the pubkey of the jsbot, a wait and a cleanup function
func initInteropAsClient(t *testing.T, jsbefore, jsafter string, sbotOpts ...sbot.Option) (*sbot.Sbot, *ssb.FeedRef, int, <-chan bool, <-chan error) {
	sbot, errc := initSbot(t, sbotOpts...)
	// r := require.New(t)

	alice, port, done, nodeErrc := startJSBotServ(t,
		jsbefore,
		jsafter,
		sbot.KeyPair.Id.Ref(),
	)

	return sbot, alice, port, done, mergeErrorChans(nodeErrc, errc)
}

// returns the created go-sbot, the pubkey of the jsbot, a wait and a cleanup function
func initInterop(t *testing.T, jsbefore, jsafter string, sbotOpts ...sbot.Option) (*sbot.Sbot, *ssb.FeedRef, <-chan bool, <-chan error, func()) {
	sbot, errc := initSbot(t, sbotOpts...)

	alice, done, nodeErrc := startJSBotClient(t,
		jsbefore,
		jsafter,
		sbot.KeyPair.Id.Ref(),
		netwrap.GetAddr(sbot.Node.GetListenAddr(), "tcp").String())

	return sbot, alice, done, mergeErrorChans(nodeErrc, errc), func() {
		<-done
	}
}

// returns the jsbots pubkey, a wait func and a done channel
func startJSBotClient(t *testing.T, jsbefore, jsafter, goRef, goAddr string) (*ssb.FeedRef, <-chan bool, <-chan error) {
	r := require.New(t)
	cmd := exec.Command("node", "./sbot_client.js")
	if testing.Verbose() {
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stderr = logtest.Logger("js", t)
	}
	outrc, err := cmd.StdoutPipe()
	r.NoError(err)

	cmd.Env = []string{
		"TEST_NAME=" + t.Name(),
		"TEST_BOB=" + goRef,
		"TEST_GOADDR=" + goAddr,
		"TEST_BEFORE=" + writeFile(t, jsbefore),
		"TEST_AFTER=" + writeFile(t, jsafter),
	}

	r.NoError(cmd.Start(), "failed to init test js-sbot")

	var done = make(chan bool)
	var errc = make(chan error, 1)
	go func() {
		err := cmd.Wait()
		if err != nil {
			errc <- errors.Wrap(err, "cmd wait failed")
		}
		close(done)
		fmt.Fprintf(os.Stderr, "\nJS Sbot process returned\n")
		close(errc)
	}()

	pubScanner := bufio.NewScanner(outrc) // TODO muxrpc comms?
	r.True(pubScanner.Scan(), "multiple lines of output from js - expected #1 to be alices pubkey/id")

	alice, err := ssb.ParseFeedRef(pubScanner.Text())
	r.NoError(err, "failed to get alice key from JS process")
	t.Logf("JS alice: %s", alice.Ref())
	return alice, done, errc
}

// returns the jsbots pubkey, a wait func and a done channel
func startJSBotServ(t *testing.T, jsbefore, jsafter, goRef string) (*ssb.FeedRef, int, <-chan bool, <-chan error) {
	r := require.New(t)
	cmd := exec.Command("node", "./sbot_serv.js")
	if testing.Verbose() {
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stderr = logtest.Logger("js", t)
	}
	outrc, err := cmd.StdoutPipe()
	r.NoError(err)
	var port = 1024 + rand.Intn(23000)
	cmd.Env = []string{
		"TEST_NAME=" + t.Name(),
		"TEST_BOB=" + goRef,
		fmt.Sprintf("TEST_PORT=%d", port),
		"TEST_BEFORE=" + writeFile(t, jsbefore),
		"TEST_AFTER=" + writeFile(t, jsafter),
	}

	r.NoError(cmd.Start(), "failed to init test js-sbot")

	var done = make(chan bool)
	var errc = make(chan error, 1)
	go func() {
		err := cmd.Wait()
		if err != nil {
			errc <- errors.Wrap(err, "cmd wait failed")
		}
		close(done)
		fmt.Fprintf(os.Stderr, "\nJS Sbot process returned\n")
		close(errc)
	}()

	pubScanner := bufio.NewScanner(outrc) // TODO muxrpc comms?
	r.True(pubScanner.Scan(), "multiple lines of output from js - expected #1 to be alices pubkey/id")

	alice, err := ssb.ParseFeedRef(pubScanner.Text())
	r.NoError(err, "failed to get alice key from JS process")
	t.Logf("JS alice: %s port: %d", alice.Ref(), port)
	return alice, port, done, errc
}

// utils
func mergeErrorChans(cs ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	out := make(chan error, 1)

	output := func(c <-chan error) {
		for a := range c {
			out <- a
		}
		wg.Done()
	}

	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
