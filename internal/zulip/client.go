// Package zulip is a minimal REST client for a Zulip organization,
// authenticated as a bot (email + API key), covering exactly what the
// bridge needs: reading recent messages and sending one into a
// stream/topic.
package zulip

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Message is a Zulip message as returned by GET /api/v1/messages.
type Message struct {
	ID               int64  `json:"id"`
	SenderID         int64  `json:"sender_id"`
	SenderEmail      string `json:"sender_email"`
	SenderFullName   string `json:"sender_full_name"`
	Type             string `json:"type"` // "stream" or "private"
	StreamID         int64  `json:"stream_id"`
	DisplayRecipient any    `json:"display_recipient"` // stream name (string) for type=="stream"; ignored for private
	Subject          string `json:"subject"`           // the topic — Zulip's message API still calls it "subject"
	Content          string `json:"content"`           // raw markdown, since requests set apply_markdown=false
	Timestamp        int64  `json:"timestamp"`
}

// Client is a minimal Zulip client authenticated as a bot.
type Client struct {
	Site   string // e.g. "https://zulip.example.org"
	Email  string // bot email, e.g. "graft-bot@zulip.example.org"
	APIKey string
	HTTP   *http.Client
}

// New builds a Zulip client.
func New(site, email, apiKey string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: 20 * time.Second}
	}
	return &Client{Site: strings.TrimRight(site, "/"), Email: email, APIKey: apiKey, HTTP: hc}
}

// LatestMessages returns the most recent limit messages across every
// stream the bot can see, oldest referenced first is NOT guaranteed —
// callers sort/group as needed. apply_markdown=false so Content is the
// original markdown, not rendered HTML (matching Discourse's Post.Raw).
func (c *Client) LatestMessages(ctx context.Context, limit int) ([]Message, error) {
	q := url.Values{}
	q.Set("anchor", "newest")
	q.Set("num_before", strconv.Itoa(limit))
	q.Set("num_after", "0")
	q.Set("narrow", "[]")
	q.Set("apply_markdown", "false")

	var out struct {
		Messages []Message `json:"messages"`
		Result   string    `json:"result"`
		Msg      string    `json:"msg"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v1/messages?"+q.Encode(), nil, &out); err != nil {
		return nil, err
	}
	if out.Result != "success" {
		return nil, fmt.Errorf("get messages: %s", out.Msg)
	}
	return out.Messages, nil
}

// SendMessage posts content into a stream/topic, addressing the stream by
// id (stable across a rename, unlike its name).
func (c *Client) SendMessage(ctx context.Context, streamID int64, topic, content string) (Message, error) {
	form := url.Values{}
	form.Set("type", "stream")
	form.Set("to", strconv.FormatInt(streamID, 10))
	form.Set("topic", topic)
	form.Set("content", content)

	var out struct {
		ID     int64  `json:"id"`
		Result string `json:"result"`
		Msg    string `json:"msg"`
	}
	if err := c.doForm(ctx, "/api/v1/messages", form, &out); err != nil {
		return Message{}, err
	}
	if out.Result != "success" {
		return Message{}, fmt.Errorf("send message: %s", out.Msg)
	}
	return Message{ID: out.ID, StreamID: streamID, Subject: topic, Content: content}, nil
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.Site+path, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.SetBasicAuth(c.Email, c.APIKey)
	req.Header.Set("Accept", "application/json")
	return c.doRequest(req, out)
}

func (c *Client) doForm(ctx context.Context, path string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Site+path, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.SetBasicAuth(c.Email, c.APIKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	return c.doRequest(req, out)
}

func (c *Client) doRequest(req *http.Request, out any) error {
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return fmt.Errorf("%s %s: HTTP %d: %s", req.Method, req.URL.Path, resp.StatusCode, truncateForLog(b))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
}

// truncateForLog bounds how much of a Zulip error body lands in an error
// string, so a large HTML error page doesn't bloat logs.
func truncateForLog(b []byte) string {
	const max = 500
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "... (truncated)"
}
