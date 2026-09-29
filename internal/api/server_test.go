package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/store"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

func populatedStore(t *testing.T) *store.Store {
	t.Helper()
	st := store.New(1)
	ctx := context.Background()
	for h := int64(1); h <= 3; h++ {
		b := types.Block{
			Height:   h,
			Hash:     fmt.Sprintf("h%d", h),
			Parent:   fmt.Sprintf("h%d", h-1),
			Producer: "v",
		}
		if _, err := st.Put(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func TestGetBlockByHeight(t *testing.T) {
	srv := httptest.NewServer(NewServer(populatedStore(t)).Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/blocks/2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200", resp.StatusCode)
	}
	var got blockResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Height != 2 {
		t.Fatalf("height=%d want 2", got.Height)
	}
}

func TestGetBlockMissing(t *testing.T) {
	srv := httptest.NewServer(NewServer(populatedStore(t)).Handler())
	defer srv.Close()
	for _, path := range []string{"/blocks/99", "/blocks/abc", "/blocks/0"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("path %s unexpectedly 200", path)
		}
		resp.Body.Close()
	}
}

func TestLatestAndTip(t *testing.T) {
	srv := httptest.NewServer(NewServer(populatedStore(t)).Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/blocks/latest")
	if err != nil {
		t.Fatal(err)
	}
	var b blockResponse
	_ = json.NewDecoder(resp.Body).Decode(&b)
	resp.Body.Close()
	if b.Height != 3 {
		t.Fatalf("latest height=%d want 3", b.Height)
	}

	resp2, err := http.Get(srv.URL + "/tip")
	if err != nil {
		t.Fatal(err)
	}
	var tip map[string]int64
	_ = json.NewDecoder(resp2.Body).Decode(&tip)
	resp2.Body.Close()
	if tip["tip"] != 3 || tip["confirmed_tip"] != 2 {
		t.Fatalf("tip payload=%v want tip=3 confirmed=2", tip)
	}
}
