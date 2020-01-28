package sbot

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"golang.org/x/sync/errgroup"

	"go.cryptoscope.co/ssb"
	// "go.cryptoscope.co/ssb/network"
	"go.cryptoscope.co/ssb/internal/mutil"
	"go.cryptoscope.co/ssb/internal/testutils"
)

func TestFeedsLiveNetworkChain(t *testing.T) {
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
		WithHops(3),
	}

	theBots := []*Sbot{}
	n := 4
	for i := 0; i < n; i++ {

		botN := makeNamedTestBot(t, strconv.Itoa(i), netOpts)
		botgroup.Go(bs.Serve(botN))

		theBots = append(theBots, botN)
	}

	followMatrix := []int{
		0, 1, 1, 1,
		1, 0, 1, 1,
		1, 1, 0, 1,
		1, 1, 1, 0,
	}

	msgCnt := int64(0)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			x := i*n + j
			fQ := followMatrix[x]

			botI := theBots[i]
			botJ := theBots[j]

			if fQ == 1 {
				msgCnt++
				t.Log(i, "follows", j)
				_, err := botI.PublishLog.Append(ssb.Contact{Type: "contact", Following: true,
					Contact: botJ.KeyPair.Id,
				})
				r.NoError(err)
			}
		}
	}

	// initialSync
	for z := 1 + n/2; z >= 0; z-- {
		// connectCtx, firstSync := context.WithCancel(ctx)

		for m := n - 1; m >= 0; m-- {
			for i := 0; i < n; i++ {
				if i == m {
					continue
				}
				err := theBots[m].Network.Connect(ctx, theBots[i].Network.GetListenAddr())
				r.NoError(err)
			}
		}
		t.Log(z, "connect..")
		time.Sleep(2 * time.Second)
		for i, bot := range theBots {
			st, err := bot.Status()
			r.NoError(err)
			if rootSeq := st.Root.Seq(); rootSeq != msgCnt-1 {
				t.Log(i, ": seq", rootSeq)
			}
		}
		// firstSync()
	}
	for i, bot := range theBots {
		st, err := bot.Status()
		r.NoError(err)
		a.EqualValues(msgCnt-1, st.Root.Seq(), "wrong rxSeq on bot %d", i)
		err = bot.FSCK(nil, FSCKModeSequences)
		a.NoError(err, "FSCK error on bot %d", i)
		bot.Network.GetConnTracker().CloseAll()
		g, err := bot.GraphBuilder.Build()
		r.NoError(err)
		err = g.RenderSVGToFile(filepath.Join("testrun", t.Name(), fmt.Sprintf("bot%d.svg", i)))
		r.NoError(err)
	}

	// dial up a chain
	for i := 0; i < n-1; i++ {
		botI := theBots[i]
		botJ := theBots[i+1]

		err := botI.Network.Connect(ctx, botJ.Network.GetListenAddr())
		r.NoError(err)
		time.Sleep(1 * time.Second)
	}

	// did b0 get feed of bN-1?
	feedIndexOfBot0, ok := theBots[0].GetMultiLog("userFeeds")
	r.True(ok)
	feedOfLastBot, err := feedIndexOfBot0.Get(theBots[n-1].KeyPair.Id.StoredAddr())
	r.NoError(err)
	seqv, err := feedOfLastBot.Seq().Value()
	r.NoError(err)
	wantSeq := margaret.BaseSeq(2)
	r.EqualValues(wantSeq, seqv, "after connect check")

	// setup live listener
	gotMsg := make(chan int64)

	seqSrc, err := mutil.Indirect(theBots[0].RootLog, feedOfLastBot).Query(
		margaret.Gt(wantSeq),
		margaret.Live(true))
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

			seq, ok := seqV.(ssb.Message)
			if !ok {
				return fmt.Errorf("wrong type: %T", seqV)
			}
			info.Log("rxFeed", seq.Seq(), "k", seq.Key().Ref())
			gotMsg <- seq.Seq()
		}
		return nil
	})

	// now publish on C and let them bubble to A, live without reconnect
	for i := 0; i < 5; i++ {
		rxSeq, err := theBots[n-1].PublishLog.Append(fmt.Sprintf("some test msg:%02d", n))
		r.NoError(err)
		a.EqualValues(margaret.BaseSeq(2+i), rxSeq)

		// received new message?
		select {
		case <-time.After(2 * time.Second):
			t.Errorf("timeout %d....", i)
		case seq := <-gotMsg:
			a.EqualValues(margaret.BaseSeq(3+i), seq, "wrong seq")
		}
	}

	// cleanup
	cancel()
	for _, bot := range theBots {
		err = bot.FSCK(nil, FSCKModeSequences)
		a.NoError(err)
		bot.Shutdown()
		r.NoError(bot.Close())
	}
	r.NoError(botgroup.Wait())
}

func TestFeedsLiveNetworkStar(t *testing.T) {
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

	theBots := []*Sbot{botA, botB, botC}

	followMatrix := []int{
		0, 1, 1,
		1, 0, 1,
		1, 1, 0,
	}

	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			x := i*3 + j
			fQ := followMatrix[x]
			// t.Log(i, j, fQ)

			botI := theBots[i]
			botJ := theBots[j]

			if fQ == 1 {
				_, err := botI.PublishLog.Append(ssb.Contact{Type: "contact", Following: true,
					Contact: botJ.KeyPair.Id,
				})
				r.NoError(err)
			}
		}

	}

	// setup listener
	uf, ok := botA.GetMultiLog("userFeeds")
	r.True(ok)
	feedOfBotC, err := uf.Get(botC.KeyPair.Id.StoredAddr())
	r.NoError(err)

	seqv, err := feedOfBotC.Seq().Value()
	r.NoError(err)
	r.EqualValues(margaret.BaseSeq(-1), seqv, "before connect check")

	// dial up A->B and B->C
	err = botA.Network.Connect(ctx, botB.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(1 * time.Second)
	err = botB.Network.Connect(ctx, botC.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(1 * time.Second)
	// hmm.. need to sync the full graph first?
	err = botA.Network.Connect(ctx, botB.Network.GetListenAddr())
	r.NoError(err)
	time.Sleep(1 * time.Second)

	// did B get feed C?
	ufOfBotB, ok := botB.GetMultiLog("userFeeds")
	r.True(ok)
	feedOfBotCAtB, err := ufOfBotB.Get(botC.KeyPair.Id.StoredAddr())
	r.NoError(err)
	seqv, err = feedOfBotCAtB.Seq().Value()
	r.NoError(err)
	r.EqualValues(margaret.BaseSeq(1), seqv, "after connect check")

	// also connect A to C directy to have two paths and filter duplicates
	// time.Sleep(3 * time.Second)
	// err = botA.Network.Connect(ctx, botC.Network.GetListenAddr())
	// r.NoError(err)
	// time.Sleep(1 * time.Second)

	// setup live listener
	gotMsg := make(chan int64)

	seqSrc, err := feedOfBotC.Query(margaret.Gte(margaret.BaseSeq(2)), margaret.Live(true))
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
			info.Log("rxFeedC", seq.Seq())
			gotMsg <- seq.Seq()
		}
		return nil
	})

	// now publish on C and let them bubble to A, live without reconnect
	for i := 0; i < 50; i++ {
		seq, err := botC.PublishLog.Append("some test msg")
		r.NoError(err)
		r.Equal(margaret.BaseSeq(6+i), seq)

		// received new message?
		select {
		case <-time.After(2 * time.Second):
			t.Errorf("timeout %d....", i)
		case seq := <-gotMsg:
			a.EqualValues(margaret.BaseSeq(2+i), seq, "wrong seq")
		}
	}

	// cleanup
	cancel()
	for _, bot := range theBots {
		err = bot.FSCK(nil, FSCKModeSequences)
		a.NoError(err)
		bot.Shutdown()
		r.NoError(bot.Close())
	}
	r.NoError(botgroup.Wait())
}

func TestFeedsLiveNetworkDiamond(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)
	os.RemoveAll(filepath.Join("testrun", t.Name()))

	ctx, testCancel := context.WithCancel(context.TODO())
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
		WithInfo(info),
		WithHops(3),
	}

	theBots := []*Sbot{}
	for n := 0; n < 6; n++ {

		botN := makeNamedTestBot(t, strconv.Itoa(n), netOpts)
		botgroup.Go(bs.Serve(botN))

		theBots = append(theBots, botN)
	}

	followMatrix := []int{
		0, 1, 1, 0, 0, 1,
		1, 0, 1, 1, 1, 0,
		1, 1, 0, 1, 1, 0,
		0, 1, 1, 0, 1, 1,
		0, 1, 1, 1, 0, 1,
		1, 0, 0, 1, 1, 0,
	}
	followMsgs := 0
	for i := 0; i < 6; i++ {
		for j := 0; j < 6; j++ {

			x := i*6 + j
			fQ := followMatrix[x]
			// t.Log(i, j, fQ)

			botI := theBots[i]
			botJ := theBots[j]

			if fQ == 1 {
				ref, err := botI.PublishLog.Publish(ssb.Contact{Type: "contact", Following: true,
					Contact: botJ.KeyPair.Id,
				})
				r.NoError(err)
				t.Log(i, "followed", j, ref.Ref()[1:5])
				followMsgs++
			}
		}
	}

	// initial sync
	// initialSyncCtx, initialSync := context.WithCancel(ctx)
	for z := 8; z >= 0; z-- {
		// connectCtx, firstSync := context.WithCancel(ctx)

		for n := 5; n >= 0; n-- {
			for i := 0; i < 6; i++ {
				if i == n {
					continue
				}
				err := theBots[n].Network.Connect(ctx, theBots[i].Network.GetListenAddr())
				r.NoError(err)
			}
		}
		t.Log(z, "connect..")
		time.Sleep(2 * time.Second)
		for i, bot := range theBots {
			st, err := bot.Status()
			r.NoError(err)
			if rootSeq := st.Root.Seq(); rootSeq != 21 {
				t.Log(i, ": seq", rootSeq)
			}
		}
		// firstSync()
	}

	// initialSync()
	for i, bot := range theBots {
		st, err := bot.Status()
		r.NoError(err)
		a.EqualValues(21, st.Root.Seq(), "wrong seq on %d", i)
		bot.Network.GetConnTracker().CloseAll()
		// g, err := bot.GraphBuilder.Build()
		// r.NoError(err)
		// err = g.RenderSVGToFile(filepath.Join("testrun", t.Name(), fmt.Sprintf("bot%d.svg", i)))
		// r.NoError(err)
	}

	// setup connections
	connectMatrix := []int{
		0, 1, 0, 0, 0, 0,
		0, 0, 1, 0, 1, 0,
		0, 0, 0, 1, 0, 0,
		0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0,
	}

	for i := 0; i < 6; i++ {
		for j := 0; j < 6; j++ {

			x := i*6 + j
			fQ := connectMatrix[x]
			// t.Log(i, j, fQ)

			botI := theBots[i]
			botJ := theBots[j]

			if fQ == 1 {
				err := botI.Network.Connect(ctx, botJ.Network.GetListenAddr())
				r.NoError(err)
				t.Log(i, "connected", j)
				time.Sleep(1 * time.Second)
			}
		}
	}

	// now send them off
	uf, ok := theBots[0].GetMultiLog("userFeeds")
	r.True(ok)
	feedOfBotC, err := uf.Get(theBots[5].KeyPair.Id.StoredAddr())
	r.NoError(err)
	// setup live listener
	gotMsg := make(chan int64)

	seqSrc, err := feedOfBotC.Query(
		margaret.Gte(margaret.BaseSeq(3)),
		margaret.Live(true))
	r.NoError(err)

	botgroup.Go(func() error {
		for {
			seqV, err := seqSrc.Next(ctx)
			t.Log(err, seqV)
			if err != nil {
				if luigi.IsEOS(err) || errors.Cause(err) == context.Canceled {
					break
				}
				return err
			}

			seq := seqV.(margaret.Seq)
			info.Log("rxFeedC", seq.Seq())
			gotMsg <- seq.Seq()
		}
		return nil
	})

	// now publish on C and let them bubble to A, live without reconnect
	for i := 0; i < 50; i++ {
		tMsg := fmt.Sprintf("some test msg %d", i)
		seq, err := theBots[5].PublishLog.Append(tMsg)
		r.NoError(err)

		r.Equal(margaret.BaseSeq(22+i), seq, "new msg %d", i)

		// received new message?
		select {
		case <-time.After(3 * time.Second):
			t.Errorf("timeout %d....", i)
		case seq := <-gotMsg:
			a.EqualValues(margaret.BaseSeq(3+i), seq, "wrong seq")
		}
	}

	// cleanup
	for i, bot := range theBots {
		err = bot.FSCK(nil, FSCKModeSequences)
		a.NoError(err, "fsck of bot %d failed", i)
	}
	testCancel()
	for _, bot := range theBots {
		bot.Shutdown()
		r.NoError(bot.Close())
	}
	r.NoError(botgroup.Wait())
}
