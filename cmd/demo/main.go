// Command demo runs the local block indexer end to end without any real chain
// or external service: it starts three in-process mock nodes, syncs their
// blocks into local storage, serves the read API, and then demonstrates a node
// outage, recovery, and a chain reorg.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/api"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/health"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/rpc"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/store"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/sync"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

func main() {
	logger := log.New(os.Stdout, "indexer ", log.LstdFlags|log.Lmicroseconds)

	// 1) Three local mock nodes serving the same linear chain of 8 blocks.
	var mocks []*rpc.MockNode
	var clients []sync.NodeClient
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%c", 'A'+i)
		m := rpc.NewMockNode(id, 8)
		srv := m.Start()
		mocks = append(mocks, m)
		clients = append(clients, rpc.NewClient(id, srv.URL, 300*time.Millisecond))
		logger.Printf("started mock %s at %s", id, srv.URL)
	}
	defer func() {
		for _, m := range mocks {
			m.Close()
		}
	}()

	// 2) Storage: 2 confirmations (heights with >=2 blocks on top are final).
	st := store.New(2)

	cfg := sync.Config{
		PollInterval:   50 * time.Millisecond,
		RequestTimeout: 300 * time.Millisecond,
		Concurrency:    4,
		BatchHeight:    64,
		Health: health.Config{
			MaxConsecutiveFail: 3,
			ProbeInterval:      100 * time.Millisecond,
			ProbeSuccessNeeded: 1,
			Logger:             logger,
		},
		Logger: logger,
	}
	syncer := sync.New(clients, st, cfg)

	// 3) Read API.
	httpSrv := &http.Server{
		Addr:    "127.0.0.1:18080",
		Handler: api.NewServer(st).WithHealth(syncer.Health()).Handler(),
	}
	go func() {
		logger.Printf("read API listening on http://%s (GET /blocks/{height}, /tip, /status)", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("http server error: %v", err)
		}
	}()

	// 4) Run the syncer until interrupted.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := syncer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Printf("syncer stopped: %v", err)
		}
	}()

	// 5) Scripted scenario: outage, recovery, chain growth, reorg.
	scenario(ctx, logger, mocks, st)

	<-ctx.Done()
	logger.Printf("shutting down; final tip=%d confirmed_tip=%d", st.Tip(), st.ConfirmedTip())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

func scenario(ctx context.Context, logger *log.Logger, mocks []*rpc.MockNode, st *store.Store) {
	waitTip := func(want int64, what string) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if st.Tip() >= want {
				logger.Printf(">> %s: tip=%d", what, st.Tip())
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		logger.Printf(">> WARNING timeout waiting for %s (tip=%d)", what, st.Tip())
	}

	waitTip(8, "initial sync complete")

	// Node A goes down (times out on every request). Sync must keep progressing
	// via B and C, then eject A and keep probing it.
	logger.Printf(">> scenario: node-A now timing out on all requests")
	mocks[0].SetBehavior(rpc.Behavior{FailAll: true, FailKind: types.ErrTimeout})
	time.Sleep(600 * time.Millisecond)
	logger.Printf(">> scenario: node-A recovers")
	mocks[0].ResetBehavior()
	time.Sleep(400 * time.Millisecond)

	// All nodes extend their chain by two blocks (still canonical).
	logger.Printf(">> scenario: chain grows +2 canonical blocks on every node")
	for _, m := range mocks {
		m.ExtendChain()
		m.ExtendChain()
	}
	waitTip(10, "caught up after chain growth")

	// Every node switches to a fork above height 8: reorg rewinds 9..10 and
	// refills alt blocks.
	logger.Printf(">> scenario: network reorg above height 8 (h9,h10 replaced)")
	for _, m := range mocks {
		m.SetChain(rpc.ForkChain(10, 8))
	}
	time.Sleep(800 * time.Millisecond)
	if b, ok := st.Get(ctx, 10); ok {
		logger.Printf(">> after reorg height 10 hash=%s (expect %s)", b.Hash, rpc.AltHashOf(10))
	}
	logger.Printf(">> scenario finished; query GET http://127.0.0.1:18080/blocks/10 or /status")
}
