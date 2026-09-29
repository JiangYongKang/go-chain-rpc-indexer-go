package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/highcumontoa/go-chain-rpc-indexer-go/internal/types"
)

// Client fetches blocks from a single node endpoint.
type Client struct {
	id      string
	baseURL string
	http    *http.Client
	timeout time.Duration
}

// NewClient builds a node client for the given endpoint. A timeout of 0 means
// the caller context alone controls deadlines.
func NewClient(id, url string, timeout time.Duration) *Client {
	return &Client{
		id:      id,
		baseURL: url,
		timeout: timeout,
		http:    &http.Client{Timeout: timeout},
	}
}

// ID returns the node identifier.
func (c *Client) ID() string { return c.id }

type wireError struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func (c *Client) do(ctx context.Context, path string) ([]byte, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, &types.NodeError{Kind: types.ErrNodeInternal, NodeID: c.id, Message: err.Error()}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// Transport-level failure: timeout vs. connection error.
		if errors.Is(err, context.DeadlineExceeded) || isTimeoutErr(ctx) {
			return nil, &types.NodeError{Kind: types.ErrTimeout, NodeID: c.id, Message: err.Error()}
		}
		return nil, &types.NodeError{Kind: types.ErrNodeInternal, NodeID: c.id, Message: err.Error()}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusOK {
		return body, nil
	}
	return nil, classify(c.id, resp.StatusCode, body)
}

func isTimeoutErr(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.DeadlineExceeded)
}

func classify(id string, status int, body []byte) error {
	kind := types.ErrNodeInternal
	var we wireError
	if json.Unmarshal(body, &we) == nil {
		switch types.ErrKind(we.Kind) {
		case types.ErrTimeout, types.ErrRateLimited, types.ErrNodeInternal, types.ErrChainBusiness:
			kind = types.ErrKind(we.Kind)
		}
	} else {
		switch {
		case status == http.StatusTooManyRequests:
			kind = types.ErrRateLimited
		case status == http.StatusGatewayTimeout:
			kind = types.ErrTimeout
		case status >= 500:
			kind = types.ErrNodeInternal
		case status >= 400:
			kind = types.ErrChainBusiness
		}
	}
	msg := we.Message
	if msg == "" {
		msg = fmt.Sprintf("http %d", status)
	}
	return &types.NodeError{Kind: kind, NodeID: id, Message: msg}
}

// GetBlock fetches a block at the given height.
func (c *Client) GetBlock(ctx context.Context, height int64) (types.Block, error) {
	body, err := c.do(ctx, fmt.Sprintf("/block?height=%d", height))
	if err != nil {
		return types.Block{}, err
	}
	var b types.Block
	if err := json.Unmarshal(body, &b); err != nil {
		return types.Block{}, &types.NodeError{Kind: types.ErrNodeInternal, NodeID: c.id, Message: "decode block: " + err.Error()}
	}
	if b.Height != height {
		return types.Block{}, &types.NodeError{
			Kind:    types.ErrChainBusiness,
			NodeID:  c.id,
			Message: fmt.Sprintf("height mismatch: asked %d got %d", height, b.Height),
		}
	}
	return b, nil
}

// LatestHeight asks the node for its current chain tip.
func (c *Client) LatestHeight(ctx context.Context) (int64, error) {
	body, err := c.do(ctx, "/tip")
	if err != nil {
		return 0, err
	}
	var tip struct {
		Height int64 `json:"height"`
	}
	if err := json.Unmarshal(body, &tip); err != nil {
		return 0, &types.NodeError{Kind: types.ErrNodeInternal, NodeID: c.id, Message: "decode tip: " + err.Error()}
	}
	return tip.Height, nil
}
