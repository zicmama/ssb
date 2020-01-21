package sbot

import (
	"context"
	"crypto/rand"
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
	"go.cryptoscope.co/ssb/network"
	"go.cryptoscope.co/ssb/internal/testutils"
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

func TestFeedsLiveNetwork(t *testing.T) {
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
	time.Sleep(2 * time.Second)

	// did B get feed C?
	ufOfBotB, ok := botB.GetMultiLog("userFeeds")
	r.True(ok)
	feedOfBotCAtB, err := ufOfBotB.Get(botC.KeyPair.Id.StoredAddr())
	r.NoError(err)
	seqv, err = feedOfBotCAtB.Seq().Value()
	r.NoError(err)
	r.EqualValues(margaret.BaseSeq(1), seqv, "after connect check")

	// should now have the two contact messages from C on A
	seqv, err = feedOfBotC.Seq().Value()
	r.NoError(err)
	r.EqualValues(margaret.BaseSeq(1), seqv, "after connect check")

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
	botA.Shutdown()
	botB.Shutdown()
	botC.Shutdown()
	r.NoError(botA.Close())
	r.NoError(botB.Close())
	r.NoError(botC.Close())
	r.NoError(botgroup.Wait())
}

func TestFeedsLiveSimple(t *testing.T) {
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
			mainLog.Log("v", seq.Seq())
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
			a.EqualValues(margaret.BaseSeq(1+i), seq, "wrong seq")
		}

		// time.Sleep(time.Duration(rand.Float32()*150) * time.Millisecond)
	}

	// cleanup
	cancel()
	ali.Shutdown()
	bob.Shutdown()

	r.NoError(ali.Close())
	r.NoError(bob.Close())

	r.NoError(botgroup.Wait())
}
