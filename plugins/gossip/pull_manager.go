// SPDX-License-Identifier: MIT

package gossip

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"go.cryptoscope.co/librarian"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/multilog"
	"go.cryptoscope.co/muxrpc"
	"go.cryptoscope.co/muxrpc/codec"

	"go.cryptoscope.co/ssb"
	"go.cryptoscope.co/ssb/graph"
	"go.cryptoscope.co/ssb/message"
)

type current struct {
	verify   luigi.Sink
	sequence int64
}

// pullManager can be queried for feeds that should be requested from an endpoint
type pullManager struct {
	self      *ssb.FeedRef // whoami
	gb        graph.Builder
	feedIndex multilog.MultiLog

	receiveLog margaret.Log
	append     luigi.Sink

	currentFeedState map[string]current

	hops    int
	hmacKey *[32]byte

	logger log.Logger
}

type rxSink struct {
	mu     sync.Mutex
	logger log.Logger
	append margaret.Log
}

func (snk *rxSink) Pour(ctx context.Context, val interface{}) error {
	snk.mu.Lock()
	seq, err := snk.append.Append(val)
	if err != nil {
		snk.mu.Unlock()
		return errors.Wrap(err, "failed to append verified message to rootLog")
	}
	msg := val.(ssb.Message)
	level.Warn(snk.logger).Log("receivedAsSeq", seq.Seq(), "ref", msg.Key().Ref())
	snk.mu.Unlock()
	return nil
}

func (snk *rxSink) Close() error { return nil }

func (pull pullManager) RequestFeeds(ctx context.Context, edp muxrpc.Endpoint) {

	// ssb.FeedsWithSequnce(pull.feedIndex)
	start := time.Now()

	hops := pull.gb.Hops(pull.self, pull.hops)
	if hops == nil {
		level.Warn(pull.logger).Log("event", "nil hops set")
		return
	}
	hopsLst, err := hops.List()
	if err != nil {
		level.Error(pull.logger).Log("event", "broken hops set", "err", err)
		return
	}
	for _, ref := range hopsLst {
		latestSeq, latestMsg, err := pull.getLatestSeq(*ref)
		if err != nil {
			level.Error(pull.logger).Log("event", "failed to get sequence for feed", "err", err, "fr", ref.Ref()[1:5])
			return
		}

		method := muxrpc.Method{"createHistoryStream"}
		var q = message.CreateHistArgs{
			ID:  ref,
			Seq: int64(latestSeq.Seq() + 1),
			StreamArgs: message.StreamArgs{
				Limit: -1},
		}
		q.Live = true

		// TODO: map of verify sinks for skipping!?
		snk := message.NewVerifySink(ref, latestSeq, latestMsg, pull.append, pull.hmacKey)

		storeSnk := luigi.FuncSink(func(ctx context.Context, val interface{}, err error) error {
			if err != nil {
				if luigi.IsEOS(err) {
					return nil
				}
				return err
			}
			pkt, ok := val.(*codec.Packet)
			if !ok {
				return errors.Errorf("muxrpc: unexpected codec value: %T", val)
			}

			if pkt.Flag.Get(codec.FlagEndErr) {
				return luigi.EOS{}
			}

			if !pkt.Flag.Get(codec.FlagStream) {
				return errors.Errorf("pullManager: expected stream packet")
			}

			return snk.Pour(ctx, pkt.Body)
			// curr, has := pull.currentFeedState[ref.Ref()]
			// if !has {
			// 	// verifySnk :=
			// 	curr = current{
			// 		verify:   verifySnk,
			// 		sequence: q.Seq,
			// 	}
			// 	pull.currentFeedState[ref.Ref()] = curr
			// }

			// err = curr.verify.Pour(ctx, pkt.Body)
			// if err != nil {
			// 	fmt.Println("verify pour failed:", err)
			// 	return err
			// }
			// return nil
		})

		err = edp.SunkenSource(ctx, storeSnk, method, q)
		if err != nil {
			err = errors.Wrapf(err, "fetchFeed(%s:%d) failed to create source", ref.Ref(), latestSeq.Seq())
			level.Error(pull.logger).Log("event", "create source", "err", err)
			return
		}
	}
	level.Debug(pull.logger).Log("msg", "pull inited", "count", hops.Count(), "took", time.Since(start))
}

func (pull pullManager) getLatestSeq(fr ssb.FeedRef) (margaret.Seq, ssb.Message, error) {
	userLog, err := pull.feedIndex.Get(fr.StoredAddr())
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to open sublog for user")
	}
	latest, err := userLog.Seq().Value()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to observe latest")
	}

	switch v := latest.(type) {
	case librarian.UnsetValue:
		// nothing stored, fetch from zero
		return margaret.SeqEmpty, nil, nil
	case margaret.BaseSeq:
		if v == margaret.SeqEmpty {
			return margaret.BaseSeq(0), nil, nil
		}
		if v < 0 {
			return nil, nil, errors.Errorf("pullManager: expected at least 1 message in index?! %d", v)
		}
		var latestSeq margaret.BaseSeq = v + 1 // sublog is 0-init while ssb chains start at 1

		rootLogValue, err := userLog.Get(v)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to look up root seq for latest user sublog")
		}
		msgV, err := pull.receiveLog.Get(rootLogValue.(margaret.Seq))
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed retreive stored message")
		}

		latestMsg, ok := msgV.(ssb.Message)
		if !ok {
			return nil, nil, errors.Errorf("fetch: wrong message type. expected %T - got %T", latestMsg, msgV)
		}

		// make sure our house is in order
		if hasSeq := latestMsg.Seq(); hasSeq != latestSeq.Seq() {
			return nil, nil, ssb.ErrWrongSequence{
				Ref:     &fr,
				Stored:  latestMsg,
				Logical: latestSeq}
		}

		return latestSeq, latestMsg, nil

	default:
		return nil, nil, errors.Errorf("pullManager: unexpected type in sequence log: %T", latest)
	}
}
