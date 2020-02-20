// SPDX-License-Identifier: MIT

package graph

import (
	"context"
	"encoding/json"
	"math"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"

	"go.cryptoscope.co/ssb"
)

type logBuilder struct {
	logger kitlog.Logger

	contactsLog margaret.Log

	current *Graph

	currentQueryCancel context.CancelFunc
}

// NewLogBuilder is a much nicer abstraction than the direct k:v implementation.
// most likely terribly slow though. Additionally, we have to unmarshal from stored.Raw again...
// TODO: actually compare the two with benchmarks if only to compare the 3rd!
func NewLogBuilder(logger kitlog.Logger, contacts margaret.Log) (*logBuilder, error) {
	lb := logBuilder{
		logger:      logger,
		contactsLog: contacts,
		current:     NewGraph(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go lb.startQuery(ctx)
	lb.currentQueryCancel = cancel

	time.Sleep(1 * time.Second)
	return &lb, nil
}

func (b *logBuilder) startQuery(ctx context.Context) {
	src, err := b.contactsLog.Query(margaret.Live(true))
	if err != nil {
		err = errors.Wrap(err, "failed to make live query for contacts")
		level.Error(b.logger).Log("err", err, "event", "query build failed")
		return
	}
	err = luigi.Pump(ctx, luigi.FuncSink(b.buildGraph), src)
	if err != nil {
		level.Error(b.logger).Log("err", err, "event", "graph build failed")
	}
}

// DeleteAuthor just triggers a rebuild (and expects the author to have dissapeard from the message source)
func (b *logBuilder) DeleteAuthor(who *ssb.FeedRef) error {
	b.current.Lock()
	defer b.current.Unlock()

	b.currentQueryCancel()
	b.current = nil

	ctx, cancel := context.WithCancel(context.Background())
	go b.startQuery(ctx)
	b.currentQueryCancel = cancel

	return nil
}

func (b *logBuilder) Authorizer(from *ssb.FeedRef, maxHops int) ssb.Authorizer {
	return &authorizer{
		b:       b,
		from:    from,
		maxHops: maxHops,
		log:     b.logger,
	}
}

func (b *logBuilder) Build() (*Graph, error) {
	return b.current, nil
}

func (b *logBuilder) buildGraph(ctx context.Context, v interface{}, err error) error {
	if err != nil {
		if luigi.IsEOS(err) {
			return nil
		}
		return err
	}

	b.current.Lock()
	defer b.current.Unlock()
	dg := b.current.WeightedDirectedGraph

	abs, ok := v.(ssb.Message)
	if !ok {
		err := errors.Errorf("graph/idx: invalid msg value %T", v)
		return err
	}
	// fmt.Println("processing", abs.Key())
	var c ssb.Contact
	err = json.Unmarshal(abs.ContentBytes(), &c)
	if err != nil {
		err = errors.Wrapf(err, "db/idx contacts: first json unmarshal failed (msg: %s)", abs.Key().Ref())
		return nil
	}

	author := abs.Author()
	contact := c.Contact

	if author.Equal(contact) {
		// contact self?!
		return nil
	}

	bfrom := author.StoredAddr()
	nFrom, has := b.current.lookup[bfrom]
	if !has {
		sr, err := ssb.NewStorageRef(author)
		if err != nil {
			return errors.Wrap(err, "failed to create graph node for author")
		}
		nFrom = &contactNode{dg.NewNode(), sr, ""}
		dg.AddNode(nFrom)
		b.current.lookup[bfrom] = nFrom
	}

	bto := contact.StoredAddr()
	nTo, has := b.current.lookup[bto]
	if !has {
		sr, err := ssb.NewStorageRef(contact)
		if err != nil {
			return errors.Wrap(err, "failed to create graph node for contact")
		}
		nTo = &contactNode{dg.NewNode(), sr, ""}
		dg.AddNode(nTo)
		b.current.lookup[bto] = nTo
	}

	w := math.Inf(-1)
	if c.Following {
		w = 1
	} else if c.Blocking {
		w = math.Inf(1)
	} else {
		if dg.HasEdgeFromTo(nFrom.ID(), nTo.ID()) {
			dg.RemoveEdge(nFrom.ID(), nTo.ID())
		}
		return nil
	}

	edg := simple.WeightedEdge{F: nFrom, T: nTo, W: w}
	// b.logger.Log("event", "add", "f", nFrom.ID(), "t", nTo.ID(), "w", w)
	dg.SetWeightedEdge(contactEdge{
		WeightedEdge: edg,
		isBlock:      c.Blocking,
	})

	return nil
}

func (b *logBuilder) Follows(from *ssb.FeedRef) (*StrFeedSet, error) {
	g, err := b.Build()
	if err != nil {
		return nil, errors.Wrap(err, "follows: couldn't build graph")
	}
	fb := from.StoredAddr()
	nFrom, has := g.lookup[fb]
	if !has {
		return nil, ErrNoSuchFrom{from}
	}

	nodes := g.From(nFrom.ID())

	refs := NewFeedSet(nodes.Len())

	for nodes.Next() {
		cnv := nodes.Node().(*contactNode)
		// warning - ignores edge type!
		edg := g.Edge(nFrom.ID(), cnv.ID())
		if edg.(contactEdge).Weight() == 1 {
			if err := refs.AddStored(cnv.feed); err != nil {
				return nil, err
			}
		}
	}
	return refs, nil
}

func (b *logBuilder) Hops(from *ssb.FeedRef, max int) *StrFeedSet {
	g, err := b.Build()
	if err != nil {
		panic(err)
	}
	b.current.Lock()
	defer b.current.Unlock()

	nFrom, has := g.lookup[from.StoredAddr()]
	if !has {
		panic("nope")
		fs := NewFeedSet(1)
		fs.AddRef(from)
		return fs
	}
	max++
	walked := NewFeedSet(10)
	// tracks the nodes we already recursed from (so we don't do them multiple times on common friends)
	visited := make(map[int64]struct{})

	err = b.recurseHops(g, walked, visited, nFrom, max)
	if err != nil {
		b.logger.Log("event", "error", "msg", "recurse failed", "err", err)
		panic(err)
		return nil
	}

	walked.Delete(from)
	return walked
}

func (b *logBuilder) recurseHops(g *Graph, walked *StrFeedSet, vis map[int64]struct{}, nFrom graph.Node, depth int) error {
	if depth == 0 {
		// b.logger.Log("depth", depth)
		return nil
	}

	nFromID := nFrom.ID()
	nodes := g.From(nFromID)

	_, visited := vis[nFromID]
	// b.logger.Log("outEdges", nodes.Len(), "v", visited)
	if visited {
		return nil
	}

	for nodes.Next() {

		cnv := nodes.Node().(*contactNode)
		followsID := cnv.ID()

		edg := g.Edge(nFromID, followsID)
		if edg.(contactEdge).Weight() == 1 {
			// b.logger.Log("f", "follow", "from", nFromID, "to", followsID)

			fr, err := cnv.feed.FeedRef()
			if err != nil {
				return err
			}

			if err := walked.AddRef(fr); err != nil {
				return err
			}
			// b.logger.Log("cnt", walked.Count())

			backEdg := g.Edge(followsID, nFromID)
			if backEdg != nil && backEdg.(contactEdge).Weight() == 1 {
				// b.logger.Log("f", "back", "to", nFromID, "from", followsID)
				if err := b.recurseHops(g, walked, vis, cnv, depth-1); err != nil {
					return err
				}
			}
		}
	}

	vis[nFromID] = struct{}{}

	return nil
}

func (bld *logBuilder) State(a, b *ssb.FeedRef) int {
	g, err := bld.Build()
	if err != nil {
		panic(err)
	}
	if g.Blocks(a, b) {
		return -1
	}
	if g.Follows(a, b) {
		return 1
	}
	return 0
}
