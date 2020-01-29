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
	"github.com/shurcooL/go-goon"
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

	msgRef, err := botD.PublishLog.Publish("testmsg1")
	r.NoError(err)
	t.Log("tst1:", msgRef.Ref())
	msgRef, err = botD.PublishLog.Publish("testmsg2")
	r.NoError(err)
	t.Log("tst2:", msgRef.Ref())
	msgRef, err = botD.PublishLog.Publish("testmsg3")
	r.NoError(err)
	t.Log("tst3:", msgRef.Ref())

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
				time.Sleep(time.Second / 3) // TODO: initial race protection
			}
			time.Sleep(time.Second / 5) // TODO: initial race protection
			botX.Network.GetConnTracker().CloseAll()
		}

		complete := 0
		for i, bot := range theBots {
			st, err := bot.Status()
			r.NoError(err)
			if rootSeq := st.Root.Seq(); rootSeq != 14 {
				t.Log("init sync delay on bot", i, ": seq", rootSeq)
			} else {
				complete++
			}
		}
		if len(theBots) == complete {
			t.Log("initsync done")
			break
		}
		t.Log(z, "initialSync..")
	}

	// check and disconnect
	for i, bot := range theBots {
		st, err := bot.Status()
		r.NoError(err)
		a.EqualValues(14, st.Root.Seq(), "wrong rxSeq on bot %d", i)
		err = bot.FSCK(nil, FSCKModeSequences)
		a.NoError(err, "FSCK error on bot %d", i)
		bot.Network.GetConnTracker().CloseAll()
	}

	// dial up A->B, B->C, C->D
	err = botA.Network.Connect(ctx, botB.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(time.Second / 2) // TODO: initial race protection
	err = botB.Network.Connect(ctx, botC.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(time.Second / 2) // TODO: initial race protection
	err = botC.Network.Connect(ctx, botD.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(time.Second / 2) // TODO: initial race protection

	// did B get feed C?
	ufOfBotB, ok := botB.GetMultiLog("userFeeds")
	r.True(ok)
	feedOfBotDAtB, err := ufOfBotB.Get(botD.KeyPair.Id.StoredAddr())
	r.NoError(err)
	seqv, err = feedOfBotDAtB.Seq().Value()
	r.NoError(err)
	r.EqualValues(margaret.BaseSeq(5), seqv, "after connect check")

	// should now have 5 msgs now (the two contact messages from C on A + 3 tests)
	seqv, err = feedOfBotD.Seq().Value()
	r.NoError(err)
	wantSeq := margaret.BaseSeq(5)
	r.EqualValues(wantSeq, seqv, "should have all of C's messages")

	// setup live listener
	gotMsg := make(chan int64)

	seqSrc, err := mutil.Indirect(botA.RootLog, feedOfBotD).Query(
		margaret.Gt(wantSeq),
		margaret.Live(true),
	)
	r.NoError(err)

	botgroup.Go(makeChanWaiter(ctx, seqSrc, gotMsg))

	t.Log("starting live test")

	// now publish on C and let them bubble to A, live without reconnect
	for i := 0; i < 50; i++ {
		seq, err := botD.PublishLog.Append("some test msg")
		r.NoError(err)
		r.Equal(margaret.BaseSeq(15+i), seq)

		// received new message?
		select {
		case <-time.After(5 * time.Second):
			t.Errorf("timeout %d....", i)
		case seq, ok := <-gotMsg:
			r.True(ok, "%d: gotMsg closed", i)
			a.EqualValues(margaret.BaseSeq(7+i), seq, "wrong seq on try %d", i)
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
	var msgCnt int64 = 2

	for i, bot := range bLeafs {
		_, err := bot.PublishLog.Append(ssb.NewContactFollow(botI.KeyPair.Id))
		r.NoError(err, "follow b%d>I failed", i)
		_, err = botI.PublishLog.Append(ssb.NewContactFollow(bot.KeyPair.Id))
		r.NoError(err, "follow I>b%d failed", i)
		msgCnt += 2
	}

	const extraTestMessages = 3
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

		seqSrc, err := mutil.Indirect(bot.RootLog, feedAonBotB).Query(
			margaret.Gt(seqOfFeedA),
			margaret.Live(true),
		)
		r.NoError(err)

		botgroup.Go(makeChanWaiter(ctx, seqSrc, gotMsg))
		botBreceivedNewMessage = append(botBreceivedNewMessage, gotMsg)
	}

	t.Log("starting live test")
	// connect A to I and Bi..n to I
	for i, botX := range append(bLeafs, botA) {
		err := botX.Network.Connect(ctx, botI.Network.GetListenAddr())
		r.NoError(err, "connect bot%d>I failed", i)
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < 10; i++ {
		tMsg := fmt.Sprintf("some fresh msg %d", i)
		seq, err := botA.PublishLog.Append(tMsg)
		r.NoError(err)
		r.EqualValues(msgCnt+int64(i), seq, "new msg %d", i)

		// received new message?
		// TODO: reflect on slice of chans for less sleep
		for bI, bChan := range botBreceivedNewMessage {
			select {
			case <-time.After(2 * time.Second):
				t.Errorf("botB%02d: timeout %d....", bI, i)
				st, err := bLeafs[bI].Status()
				r.NoError(err)
				goon.Dump(st)
			case seq := <-bChan:
				a.EqualValues(int(seqOfFeedA+2)+i, seq, "botB%02d: wrong seq", bI)
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

func makeChanWaiter(ctx context.Context, src luigi.Source, gotMsg chan<- int64) func() error {
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
			gotMsg <- msg.Seq()
		}
	}
}
