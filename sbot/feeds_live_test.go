package sbot

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"golang.org/x/sync/errgroup"

	"go.cryptoscope.co/ssb"
	"go.cryptoscope.co/ssb/internal/testutils"
	"go.cryptoscope.co/ssb/network"
)

func makeNamedTestBot(t *testing.T, name string, opts []Option) *Sbot {
	r := require.New(t)

	testPath := filepath.Join("testrun", t.Name(), "bot-"+name)

	mainLog := testutils.NewRelativeTimeLogger(nil) //ioutil.Discard)
	botOptions := append(opts,
		WithInfo(log.With(mainLog, "bot", name)),
		WithRepoPath(testPath),
		WithListenAddr(":0"),
		WithNetworkConnTracker(network.NewLastWinsTracker()),
	)

	theBot, err := New(botOptions...)
	r.NoError(err)
	return theBot
}

func TestFeedsLiveSimpleThree(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)
	os.RemoveAll(filepath.Join("testrun", t.Name()))

	ctx, cancel := context.WithCancel(context.TODO())
	botgroup, ctx := errgroup.WithContext(ctx)

	info := testutils.NewRelativeTimeLogger(nil)
	bs := newBotServer(ctx, info)

	appKey := make([]byte, 32)
	rand.Read(appKey)
	hmacKey := make([]byte, 32)
	rand.Read(hmacKey)

	netOpts := []Option{
		WithAppKey(appKey),
		WithHMACSigning(hmacKey),
	}

	botA := makeNamedTestBot(t, "A", netOpts)
	botgroup.Go(bs.Serve(botA))

	botB := makeNamedTestBot(t, "B", netOpts)
	botgroup.Go(bs.Serve(botB))

	botC := makeNamedTestBot(t, "C", netOpts)
	botgroup.Go(bs.Serve(botC))

	// be-friend the network
	_, err := botA.PublishLog.Append(ssb.Contact{Type: "contact", Following: true,
		Contact: botB.KeyPair.Id,
	})
	r.NoError(err)
	_, err = botA.PublishLog.Append(ssb.Contact{Type: "contact", Following: true,
		Contact: botC.KeyPair.Id,
	})
	r.NoError(err)

	_, err = botB.PublishLog.Append(ssb.Contact{Type: "contact", Following: true,
		Contact: botA.KeyPair.Id,
	})
	r.NoError(err)
	_, err = botB.PublishLog.Append(ssb.Contact{Type: "contact", Following: true,
		Contact: botC.KeyPair.Id,
	})
	r.NoError(err)

	_, err = botC.PublishLog.Append(ssb.Contact{Type: "contact", Following: true,
		Contact: botA.KeyPair.Id,
	})
	r.NoError(err)
	_, err = botC.PublishLog.Append(ssb.Contact{Type: "contact", Following: true,
		Contact: botB.KeyPair.Id,
	})
	r.NoError(err)

	msgRef, err := botC.PublishLog.Publish("testmsg1")
	r.NoError(err)
	t.Log("tst1:", msgRef.Ref())
	msgRef, err = botC.PublishLog.Publish("testmsg2")
	r.NoError(err)
	t.Log("tst2:", msgRef.Ref())
	msgRef, err = botC.PublishLog.Publish("testmsg3")
	r.NoError(err)
	t.Log("tst3:", msgRef.Ref())

	// check feed of C is empty on bot A
	uf, ok := botA.GetMultiLog("userFeeds")
	r.True(ok)
	feedOfBotC, err := uf.Get(botC.KeyPair.Id.StoredAddr())
	r.NoError(err)

	seqv, err := feedOfBotC.Seq().Value()
	r.NoError(err)
	r.EqualValues(margaret.BaseSeq(-1), seqv, "before connect check")

	// dial up A->B and B->C (initial sync)
	err = botA.Network.Connect(ctx, botB.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(time.Second / 2)
	err = botB.Network.Connect(ctx, botC.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(time.Second / 2)
	err = botA.Network.Connect(ctx, botB.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(time.Second / 2)

	// did B get feed C?
	ufOfBotB, ok := botB.GetMultiLog("userFeeds")
	r.True(ok)
	feedOfBotCAtB, err := ufOfBotB.Get(botC.KeyPair.Id.StoredAddr())
	r.NoError(err)
	seqv, err = feedOfBotCAtB.Seq().Value()
	r.NoError(err)
	r.EqualValues(margaret.BaseSeq(4), seqv, "after connect check")

	// should now have 5 msgs now (the two contact messages from C on A + 3 tests)
	seqv, err = feedOfBotC.Seq().Value()
	r.NoError(err)
	wantSeq := margaret.BaseSeq(4)
	r.EqualValues(wantSeq, seqv, "should have all of C's messages")

	// setup live listener
	gotMsg := make(chan int64)

	seqSrc, err := feedOfBotC.Query(
		margaret.Gt(wantSeq),
		margaret.Live(true),
	)
	r.NoError(err)

	botgroup.Go(func() error {
		defer close(gotMsg)
		for {
			seqV, err := seqSrc.Next(ctx)
			if err != nil {
				if luigi.IsEOS(err) || errors.Cause(err) == context.Canceled {
					t.Log("query exited", err)
					return nil
				}
				return err
			}
			seq := seqV.(margaret.Seq)
			msgV, err := botA.RootLog.Get(seq)
			if err != nil {
				return err
			}
			msg := msgV.(ssb.Message)

			t.Log("rxFeedC", seq.Seq(), "msgSeq", msg.Seq(), "key", msg.Key().Ref())
			gotMsg <- msg.Seq()
		}
	})

	t.Log("starting live test")

	time.Sleep(1 * time.Second)

	// now publish on C and let them bubble to A, live without reconnect
	for i := 0; i < 50; i++ {
		seq, err := botC.PublishLog.Append("some test msg")
		r.NoError(err)
		r.Equal(margaret.BaseSeq(9+i), seq)

		// received new message?
		select {
		case <-time.After(5 * time.Second):
			t.Errorf("timeout %d....", i)
		case seq, ok := <-gotMsg:
			r.True(ok, "%d: gotMsg closed", i)
			a.EqualValues(margaret.BaseSeq(6+i), seq, "wrong seq on try %d", i)
		}
	}

	// cleanup
	err = botA.FSCK(nil, FSCKModeSequences)
	a.NoError(err, "FSCK error on A")
	err = botB.FSCK(nil, FSCKModeSequences)
	a.NoError(err, "FSCK error on B")
	err = botC.FSCK(nil, FSCKModeSequences)
	a.NoError(err, "FSCK error on C")

	t.Log("done with cleanup")
	cancel()

	botA.Shutdown()
	botB.Shutdown()
	botC.Shutdown()
	r.NoError(botA.Close())
	r.NoError(botB.Close())
	r.NoError(botC.Close())
	r.NoError(botgroup.Wait())
}

// setup two bots, connect once and publish afterwards
func TestFeedsLiveSimpleTwo(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.TODO())

	os.RemoveAll("testrun")

	appKey := make([]byte, 32)
	rand.Read(appKey)
	hmacKey := make([]byte, 32)
	rand.Read(hmacKey)

	botgroup, ctx := errgroup.WithContext(ctx)

	mainLog := testutils.NewRelativeTimeLogger(nil)

	ali, err := New(
		WithAppKey(appKey),
		WithHMACSigning(hmacKey),
		WithContext(ctx),
		WithInfo(log.With(mainLog, "unit", "ali")),

		WithRepoPath(filepath.Join("testrun", t.Name(), "ali")),
		WithListenAddr(":0"),
	)
	r.NoError(err)

	botgroup.Go(func() error {
		err := ali.Network.Serve(ctx)
		if err != nil {
			level.Warn(mainLog).Log("event", "ali serve exited", "err", err)
		}
		if err == context.Canceled {
			return nil
		}
		return err
	})

	bob, err := New(
		WithAppKey(appKey),
		WithHMACSigning(hmacKey),
		WithContext(ctx),
		WithInfo(log.With(mainLog, "unit", "bob")),

		WithRepoPath(filepath.Join("testrun", t.Name(), "bob")),
		WithListenAddr(":0"),
	)
	r.NoError(err)

	botgroup.Go(func() error {
		err := bob.Network.Serve(ctx)
		if err != nil {
			level.Warn(mainLog).Log("event", "bob serve exited", "err", err)
		}
		if err == context.Canceled {
			return nil
		}
		return err
	})

	seq, err := ali.PublishLog.Append(ssb.Contact{
		Type:      "contact",
		Following: true,
		Contact:   bob.KeyPair.Id,
	})
	r.NoError(err)
	r.Equal(margaret.BaseSeq(0), seq)

	seq, err = bob.PublishLog.Append(ssb.Contact{
		Type:      "contact",
		Following: true,
		Contact:   ali.KeyPair.Id,
	})
	r.NoError(err)
	r.Equal(margaret.BaseSeq(0), seq)

	err = bob.Network.Connect(ctx, ali.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(1 * time.Second)

	uf, ok := bob.GetMultiLog("userFeeds")
	r.True(ok)
	alisLog, err := uf.Get(ali.KeyPair.Id.StoredAddr())
	r.NoError(err)

	seqv, err := alisLog.Seq().Value()
	r.NoError(err)
	a.Equal(margaret.BaseSeq(0), seqv, "after connect check")

	// setup live listener
	gotMsg := make(chan int64)

	seqSrc, err := alisLog.Query(margaret.Gte(margaret.BaseSeq(1)), margaret.Live(true))
	r.NoError(err)

	botgroup.Go(func() error {
		for {
			seqV, err := seqSrc.Next(ctx)
			if err != nil {
				if luigi.IsEOS(err) || errors.Cause(err) == context.Canceled {
					break
				}
				return err
			}

			seq := seqV.(margaret.Seq)
			mainLog.Log("onQuery", seq.Seq())
			gotMsg <- seq.Seq()
		}
		return nil
	})

	for i := 0; i < 50; i++ {
		seq, err = ali.PublishLog.Append("first msg after connect")
		r.NoError(err)
		r.Equal(margaret.BaseSeq(2+i), seq)

		// received new message?
		select {
		case <-time.After(2 * time.Second):
			t.Errorf("timeout %d....", i)
		case seq := <-gotMsg:
			a.EqualValues(margaret.BaseSeq(2+i), seq, "wrong seq")
		}
	}

	// cleanup
	err = ali.FSCK(nil, FSCKModeSequences)
	a.NoError(err, "FSCK error on A")

	err = bob.FSCK(nil, FSCKModeSequences)
	a.NoError(err, "FSCK error on B")
	cancel()
	ali.Shutdown()
	bob.Shutdown()

	r.NoError(ali.Close())
	r.NoError(bob.Close())

	r.NoError(botgroup.Wait())
}

// A publishes and is only connected to I
// B1 to N are connected to I and should get the fan out
func TestFeedsLiveSimpleStar(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)
	os.RemoveAll(filepath.Join("testrun", t.Name()))

	ctx, cancel := context.WithCancel(context.TODO())
	botgroup, ctx := errgroup.WithContext(ctx)

	info := testutils.NewRelativeTimeLogger(nil)
	bs := newBotServer(ctx, info)

	appKey := make([]byte, 32)
	rand.Read(appKey)
	hmacKey := make([]byte, 32)
	rand.Read(hmacKey)

	netOpts := []Option{
		WithAppKey(appKey),
		WithHMACSigning(hmacKey),
	}

	botA := makeNamedTestBot(t, "A", netOpts)
	botgroup.Go(bs.Serve(botA))

	botI := makeNamedTestBot(t, "I", netOpts)
	botgroup.Go(bs.Serve(botI))

	var bLeafs []*Sbot
	botB1 := makeNamedTestBot(t, "B1", netOpts)
	botgroup.Go(bs.Serve(botB1))
	bLeafs = append(bLeafs, botB1)

	botB2 := makeNamedTestBot(t, "B2", netOpts)
	botgroup.Go(bs.Serve(botB2))
	bLeafs = append(bLeafs, botB2)

	botB3 := makeNamedTestBot(t, "B3", netOpts)
	botgroup.Go(bs.Serve(botB3))
	bLeafs = append(bLeafs, botB3)

	theBots := []*Sbot{botA, botI} // all the bots
	theBots = append(theBots, bLeafs...)

	// be-friend the network
	_, err := botA.PublishLog.Append(ssb.NewContactFollow(botI.KeyPair.Id))
	r.NoError(err)
	_, err = botI.PublishLog.Append(ssb.NewContactFollow(botA.KeyPair.Id))
	r.NoError(err)

	for i, bot := range bLeafs {
		_, err := bot.PublishLog.Append(ssb.NewContactFollow(botI.KeyPair.Id))
		r.NoError(err, "follow b%d>I failed", i)
		_, err = botI.PublishLog.Append(ssb.NewContactFollow(bot.KeyPair.Id))
		r.NoError(err, "follow I>b%d failed", i)
	}

	const extraTestMessages = 5
	const msgCnt = 8 + extraTestMessages
	for n := extraTestMessages; n > 0; n-- {
		tMsg := fmt.Sprintf("some pre-setup msg %d", n)
		_, err := botA.PublishLog.Append(tMsg)
		r.NoError(err)
	}

	// initialSync
	for z := 3; z > 0; z-- {

		for bI, botX := range theBots {
			for bJ, botY := range theBots {
				if bI == bJ {
					continue
				}
				err := botX.Network.Connect(ctx, botY.Network.GetListenAddr())
				r.NoError(err)
			}
		}
		t.Log(z, "initialSync..")
		time.Sleep(1 * time.Second)
		for i, bot := range theBots {
			st, err := bot.Status()
			r.NoError(err)
			if rootSeq := st.Root.Seq(); rootSeq != msgCnt-1 {
				t.Log("init sync delay on bot", i, ": seq", rootSeq)
			}
		}
	}

	// check and disconnect
	for i, bot := range theBots {
		st, err := bot.Status()
		r.NoError(err)
		a.EqualValues(msgCnt-1, st.Root.Seq(), "wrong rxSeq on bot %d", i)
		err = bot.FSCK(nil, FSCKModeSequences)
		a.NoError(err, "FSCK error on bot %d", i)
		bot.Network.GetConnTracker().CloseAll()
		// g, err := bot.GraphBuilder.Build()
		// r.NoError(err)
		// err = g.RenderSVGToFile(filepath.Join("testrun", t.Name(), fmt.Sprintf("bot%d.svg", i)))
		// r.NoError(err)
	}

	seqOfFeedA := margaret.BaseSeq(extraTestMessages) // N pre messages +1 contact (0 indexed)
	var botBreceivedNewMessage []<-chan int64
	for i, bot := range bLeafs {

		// did Bi get feed A?
		ufOfBotB, ok := bot.GetMultiLog("userFeeds")
		r.True(ok)
		feedAonBotB, err := ufOfBotB.Get(botA.KeyPair.Id.StoredAddr())
		r.NoError(err)
		seqv, err := feedAonBotB.Seq().Value()
		r.NoError(err)
		a.EqualValues(seqOfFeedA, seqv, "botB%02d should have all of A's messages", i)

		// setup live listener
		gotMsg := make(chan int64)

		seqSrc, err := feedAonBotB.Query(
			margaret.Gt(seqOfFeedA),
			margaret.Live(true),
		)
		r.NoError(err)

		botgroup.Go(makeChanWaiter(ctx, bot.RootLog, seqSrc, gotMsg))
		botBreceivedNewMessage = append(botBreceivedNewMessage, gotMsg)
	}

	t.Log("starting live test")
	// connect A to I and Bi..n to I
	for i, botX := range append(bLeafs, botA) {
		err := botX.Network.Connect(ctx, botI.Network.GetListenAddr())
		r.NoError(err, "connect bot%d>I failed", i)
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < 7; i++ {
		tMsg := fmt.Sprintf("some fresh msg %d", i)
		seq, err := botA.PublishLog.Append(tMsg)
		r.NoError(err)
		r.EqualValues(msgCnt+i, seq, "new msg %d", i)

		// received new message?
		for bI, bChan := range botBreceivedNewMessage {
			select {
			case <-time.After(1 * time.Second):
				t.Errorf("botB%02d: timeout %d....", bI, i)
			case seq := <-bChan:
				a.EqualValues(int(seqOfFeedA)+i, seq, "botB%20d: wrong seq", bI)
			}
		}
	}

	// cleanup
	cancel()
	time.Sleep(1 * time.Second)
	for _, bot := range append(bLeafs, botA, botI) {
		err = bot.FSCK(nil, FSCKModeSequences)
		a.NoError(err)
		bot.Shutdown()
		r.NoError(bot.Close())
	}
	r.NoError(botgroup.Wait())
}
