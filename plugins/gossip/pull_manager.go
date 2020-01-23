// SPDX-License-Identifier: MIT

package gossip

import (
	"context"
	"encoding/json"

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

// PullManager can be queried for feeds that should be requested from an endpoint
type PullManager struct {
	self      *ssb.FeedRef // whoami
	gb        graph.Builder
	feedIndex multilog.MultiLog

	logger log.Logger

	hops int
}

func NewFeedPullManager() *PullManager {
	pm := &PullManager{}
	return pm
}

func (pull PullManager) RequestFeeds(ctx context.Context, edp muxrpc.Endpoint) {

	// ssb.FeedsWithSequnce(pull.feedIndex)

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

		latestSeq, err := pull.getLatestSeq(*ref)
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

		var src luigi.Source
		switch ref.Algo {
		case ssb.RefAlgoFeedSSB1:
			src, err = edp.Source(ctx, json.RawMessage{}, method, q)
		case ssb.RefAlgoFeedGabby:
			src, err = edp.Source(ctx, codec.Body{}, method, q)
		}
		if err != nil {
			err = errors.Wrapf(err, "fetchFeed(%s:%d) failed to create source", ref.Ref(), latestSeq.Seq())
			level.Error(pull.logger).Log("event", "create source", "err", err)
			return
		}

		// TODO
		_ = src
		// pull.storage.Drain(src)
	}

}

func (pull PullManager) getLatestSeq(fr ssb.FeedRef) (margaret.Seq, error) {
	userLog, err := pull.feedIndex.Get(fr.StoredAddr())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open sublog for user")
	}
	latest, err := userLog.Seq().Value()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to observe latest")
	}
	var (
	// latestSeq margaret.BaseSeq
	// latestMsg ssb.Message
	)
	switch v := latest.(type) {
	case librarian.UnsetValue:
		// nothing stored, fetch from zero
		return margaret.SeqEmpty, nil
	case margaret.BaseSeq:
		// latestSeq = v + 1 // sublog is 0-init while ssb chains start at 1
		return v, nil
		// if v >= 0 {
		// 	rootLogValue, err := userLog.Get(v)
		// 	if err != nil {
		// 		return nil, errors.Wrapf(err, "failed to look up root seq for latest user sublog")
		// 	}
		// 	msgV, err := g.receiveLog.Get(rootLogValue.(margaret.Seq))
		// 	if err != nil {
		// 		return nil, errors.Wrapf(err, "failed retreive stored message")
		// 	}

		// 	var ok bool
		// 	latestMsg, ok = msgV.(ssb.Message)
		// 	if !ok {
		// 		return errors.Errorf("fetch: wrong message type. expected %T - got %T", latestMsg, msgV)
		// 	}

		// 	// make sure our house is in order
		// 	if hasSeq := latestMsg.Seq(); hasSeq != latestSeq.Seq() {
		// 		return nil, ssb.ErrWrongSequence{Ref: fr, Stored: latestMsg, Logical: latestSeq}
		// 	}
		// }
	default:
		return nil, errors.Errorf("pullManager: unexpected type in sequence log: %T", latest)
	}
}
