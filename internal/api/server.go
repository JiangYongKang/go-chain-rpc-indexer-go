// Package api exposes the read interface of the indexer over HTTP.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/health"
	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/store"
)

// Server serves block read endpoints backed by the local store.
type Server struct {
	store  *store.Store
	health *health.Monitor
}

// NewServer builds an API server over st.
func NewServer(st *store.Store) *Server { return &Server{store: st} }

// WithHealth attaches a health monitor exposed at /status.
func (s *Server) WithHealth(h *health.Monitor) *Server {
	s.health = h
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /blocks/latest", s.handleLatest)
	mux.HandleFunc("GET /blocks/{height}", s.handleBlock)
	mux.HandleFunc("GET /tip", s.handleTip)
	mux.HandleFunc("GET /status", s.handleStatus)
	return mux
}

type blockResponse struct {
	Height   int64  `json:"height"`
	Hash     string `json:"hash"`
	Parent   string `json:"parent"`
	Producer string `json:"producer"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleLatest(w http.ResponseWriter, _ *http.Request) {
	tip := s.store.Tip()
	if tip < 1 {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "no blocks yet"})
		return
	}
	b, ok := s.store.Get(context.Background(), tip)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "tip block not found"})
		return
	}
	writeJSON(w, http.StatusOK, blockResponse{
		Height: b.Height, Hash: b.Hash, Parent: b.Parent, Producer: b.Producer,
	})
}

func (s *Server) handleBlock(w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("height")
	h, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || h < 1 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "height must be a positive integer"})
		return
	}
	b, ok := s.store.Get(r.Context(), h)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "block not found (not yet linked or rolled back)"})
		return
	}
	writeJSON(w, http.StatusOK, blockResponse{
		Height: b.Height, Hash: b.Hash, Parent: b.Parent, Producer: b.Producer,
	})
}

func (s *Server) handleTip(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]int64{
		"tip":           s.store.Tip(),
		"confirmed_tip": s.store.ConfirmedTip(),
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	if s.health == nil {
		writeJSON(w, http.StatusOK, map[string]any{"nodes": []any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": s.health.Snapshot()})
}
