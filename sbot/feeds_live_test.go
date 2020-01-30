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
	"go.cryptoscope.co/ssb/internal/mutil"
	"go.cryptoscope.co/ssb/internal/testutils"
	"go.cryptoscope.co/ssb/network"
)

var testMessageCount = 512

func makeNamedTestBot(t *testing.T, name string, opts []Option) *Sbot {
	r := require.New(t)
	if testing.Short() {
		testMessageCount = 25
	}
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

func TestFeedsLiveSimpleFour(t *testing.T) {
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

	botD := makeNamedTestBot(t, "D", netOpts)
	botgroup.Go(bs.Serve(botD))

	// be-friend the network
	_, err := botA.PublishLog.Append(ssb.NewContactFollow(botB.KeyPair.Id))
	r.NoError(err)
	_, err = botA.PublishLog.Append(ssb.NewContactFollow(botC.KeyPair.Id))
	r.NoError(err)
	_, err = botA.PublishLog.Append(ssb.NewContactFollow(botD.KeyPair.Id))
	r.NoError(err)

	_, err = botB.PublishLog.Append(ssb.NewContactFollow(botA.KeyPair.Id))
	r.NoError(err)
	_, err = botB.PublishLog.Append(ssb.NewContactFollow(botC.KeyPair.Id))
	r.NoError(err)
	_, err = botB.PublishLog.Append(ssb.NewContactFollow(botD.KeyPair.Id))
	r.NoError(err)

	_, err = botC.PublishLog.Append(ssb.NewContactFollow(botA.KeyPair.Id))
	r.NoError(err)
	_, err = botC.PublishLog.Append(ssb.NewContactFollow(botB.KeyPair.Id))
	r.NoError(err)
	_, err = botC.PublishLog.Append(ssb.NewContactFollow(botD.KeyPair.Id))
	r.NoError(err)

	_, err = botD.PublishLog.Append(ssb.NewContactFollow(botA.KeyPair.Id))
	r.NoError(err)
	_, err = botD.PublishLog.Append(ssb.NewContactFollow(botB.KeyPair.Id))
	r.NoError(err)
	_, err = botD.PublishLog.Append(ssb.NewContactFollow(botC.KeyPair.Id))
	r.NoError(err)
	var msgCnt = 4*3 - 1

	msgCnt += testMessageCount
	for n := testMessageCount; n > 0; n-- {
		tMsg := fmt.Sprintf("some pre-setup msg %d", n)
		_, err := botD.PublishLog.Append(tMsg)
		r.NoError(err)
	}

	// check feed of C is empty on bot A
	uf, ok := botA.GetMultiLog("userFeeds")
	r.True(ok)
	feedOfBotD, err := uf.Get(botD.KeyPair.Id.StoredAddr())
	r.NoError(err)

	seqv, err := feedOfBotD.Seq().Value()
	r.NoError(err)
	r.EqualValues(margaret.BaseSeq(-1), seqv, "before connect check")

	// initial sync
	theBots := []*Sbot{botA, botB, botC, botD}
	for z := 3; z > 0; z-- {

		for bI, botX := range theBots {
			for bJ, botY := range theBots {
				if bI == bJ {
					continue
				}
				err := botX.Network.Connect(ctx, botY.Network.GetListenAddr())
				r.NoError(err)
				time.Sleep(time.Second / 2) // settle sync
			}
		}

		complete := 0
		for i, bot := range theBots {
			st, err := bot.Status()
			r.NoError(err)
			if rootSeq := int(st.Root.Seq()); rootSeq != msgCnt {
				t.Log("init sync diff on bot", i, ": seq", rootSeq)
			} else {
				complete++
			}
		}
		if len(theBots) == complete {
			t.Log("initsync done")
			break
		}
		t.Log(z, "continuing initialSync..")
	}

	// check and disconnect
	var broken = false
	for i, bot := range theBots {
		st, err := bot.Status()
		r.NoError(err)
		a.EqualValues(msgCnt, st.Root.Seq(), "wrong rxSeq on bot %d", i)
		err = bot.FSCK(nil, FSCKModeSequences)
		if !a.NoError(err, "FSCK error on bot %d", i) {
			broken = true
		}
		bot.Network.GetConnTracker().CloseAll()
	}
	if broken {
		t.Fatal()
	}

	// dial up A->B, B->C, C->D
	err = botA.Network.Connect(ctx, botB.Network.GetListenAddr())
	r.NoError(err)
	err = botB.Network.Connect(ctx, botC.Network.GetListenAddr())
	r.NoError(err)
	err = botC.Network.Connect(ctx, botD.Network.GetListenAddr())
	r.NoError(err)

	// did B get feed C?
	ufOfBotB, ok := botB.GetMultiLog("userFeeds")
	r.True(ok)
	feedOfBotDAtB, err := ufOfBotB.Get(botD.KeyPair.Id.StoredAddr())
	r.NoError(err)
	seqv, err = feedOfBotDAtB.Seq().Value()
	r.NoError(err)

	wantSeq := margaret.BaseSeq(testMessageCount + 2)

	r.EqualValues(wantSeq, seqv, "after connect check on bot B")

	// should now have 5 msgs now (the two contact messages from C on A + 3 tests)
	seqv, err = feedOfBotD.Seq().Value()
	r.NoError(err)
	r.EqualValues(wantSeq, seqv, "should have all of C's messages")

	// setup live listener
	gotMsg := make(chan ssb.Message)

	seqSrc, err := mutil.Indirect(botA.RootLog, feedOfBotD).Query(
		margaret.Gt(wantSeq),
		margaret.Live(true),
	)
	r.NoError(err)

	botgroup.Go(makeChanWaiter(ctx, seqSrc, gotMsg))

	t.Log("starting live test")

	// now publish on C and let them bubble to A, live without reconnect

	for i := 0; i < testMessageCount; i++ {
		rxSeq, err := botD.PublishLog.Append("some test msg")
		r.NoError(err)
		published := time.Now()
		r.Equal(margaret.BaseSeq(testMessageCount+12+i), rxSeq)

		// received new message?
		select {
		case <-time.After(time.Second / 2):
			t.Errorf("timeout %d....", i)
		case msg, ok := <-gotMsg:
			r.True(ok, "%d: gotMsg closed", i)
			a.EqualValues(margaret.BaseSeq(testMessageCount+4+i), msg.Seq(), "wrong seq on try %d", i)
			t.Log("delay:", time.Since(published))
		}
	}

	t.Log("cleanup")
	cancel()
	time.Sleep(1 * time.Second)
	for _, bot := range theBots {
		err = bot.FSCK(nil, FSCKModeSequences)
		a.NoError(err)
		bot.Shutdown()
		r.NoError(bot.Close())
	}
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
	time.Sleep(time.Second / 2)

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
	if testing.Short() {
		testMessageCount = 25
	}
	for i := 0; i < testMessageCount; i++ {
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
	for i := 0; i < 6; i++ {
		botBi := makeNamedTestBot(t, fmt.Sprintf("B%0d", i), netOpts)
		botgroup.Go(bs.Serve(botBi))
		bLeafs = append(bLeafs, botBi)
	}

	theBots := []*Sbot{botA, botI} // all the bots
	theBots = append(theBots, bLeafs...)

	// be-friend the network
	_, err := botA.PublishLog.Append(ssb.NewContactFollow(botI.KeyPair.Id))
	r.NoError(err)
	_, err = botI.PublishLog.Append(ssb.NewContactFollow(botA.KeyPair.Id))
	r.NoError(err)
	var msgCnt = 2

	for i, bot := range bLeafs {
		_, err := bot.PublishLog.Append(ssb.NewContactFollow(botI.KeyPair.Id))
		r.NoError(err, "follow b%d>I failed", i)
		_, err = botI.PublishLog.Append(ssb.NewContactFollow(bot.KeyPair.Id))
		r.NoError(err, "follow I>b%d failed", i)
		msgCnt += 2
	}

	var extraTestMessages = testMessageCount
	msgCnt += extraTestMessages
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
		complete := 0
		for i, bot := range theBots {
			st, err := bot.RootLog.Seq().Value()
			r.NoError(err)
			if rootSeq := int(st.(margaret.Seq).Seq()); rootSeq != msgCnt-1 {
				t.Log("init sync delay on bot", i, ": seq", rootSeq)
			} else {
				complete++
			}
		}
		if len(theBots) == complete {
			t.Log("initsync done")
			break
		}
		t.Log(z, "continuing initialSync..")
		time.Sleep(1 * time.Second)
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
	var botBreceivedNewMessage []<-chan ssb.Message
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
		gotMsg := make(chan ssb.Message)

		seqSrc, err := mutil.Indirect(bot.RootLog, feedAonBotB).Query(
			margaret.Gt(seqOfFeedA),
			margaret.Live(true),
		)
		r.NoError(err)

		botgroup.Go(makeChanWaiter(ctx, seqSrc, gotMsg))
		botBreceivedNewMessage = append(botBreceivedNewMessage, gotMsg)
	}

	t.Log("starting live test")
	// connect all bots to I
	for i, botX := range append(bLeafs, botA) {
		err := botX.Network.Connect(ctx, botI.Network.GetListenAddr())
		r.NoError(err, "connect bot%d>I failed", i)
	}

	for i := 0; i < testMessageCount; i++ {
		tMsg := fmt.Sprintf("some fresh msg %d", i)
		seq, err := botA.PublishLog.Append(tMsg)
		r.NoError(err)
		published := time.Now()
		r.EqualValues(msgCnt+i, seq, "new msg %d", i)

		// received new message?
		// TODO: reflect on slice of chans for less sleep
		for bI, bChan := range botBreceivedNewMessage {
			select {
			case <-time.After(time.Second / 2):
				t.Errorf("botB%02d: timeout %d....", bI, i)
				// st, err := bLeafs[bI].Status()
				// r.NoError(err)
				// goon.Dump(st)
			case msg := <-bChan:
				a.EqualValues(int(seqOfFeedA+2)+i, msg.Seq(), "botB%02d: wrong seq", bI)
				t.Log("delay:", time.Since(published))
			}
		}
	}

	// cleanup
	cancel()
	time.Sleep(1 * time.Second)
	for bI, bot := range append(bLeafs, botA, botI) {
		err = bot.FSCK(nil, FSCKModeSequences)
		a.NoError(err, "botB%02d fsck", bI)
		bot.Shutdown()
		r.NoError(bot.Close())
	}
	r.NoError(botgroup.Wait())
}

func makeChanWaiter(ctx context.Context, src luigi.Source, gotMsg chan<- ssb.Message) func() error {
	return func() error {
		defer close(gotMsg)
		for {
			v, err := src.Next(ctx)
			if err != nil {
				if luigi.IsEOS(err) || errors.Cause(err) == context.Canceled {
					fmt.Println("query exited", err)
					return nil
				}
				return err
			}

			msg := v.(ssb.Message)

			fmt.Println("rxFeed", msg.Author().Ref()[1:5], "msgSeq", msg.Seq(), "key", msg.Key().Ref())
			gotMsg <- msg
		}
	}
}
