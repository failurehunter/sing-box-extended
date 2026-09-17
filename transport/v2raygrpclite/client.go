package v2raygrpclite

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2rayhttp"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"golang.org/x/net/http2"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)

var defaultClientHeader = http.Header{
	"Content-Type": []string{"application/grpc"},
	"User-Agent":   []string{"grpc-go/1.48.0"},
	"TE":           []string{"trailers"},
}

// Keepalive parameters mirroring google.golang.org/grpc:
//   - keepaliveMinPingTime: internal.KeepaliveMinPingTime (10s), applied when
//     clamping kp.Time in grpc.WithKeepaliveParams.
//   - defaultClientKeepaliveTimeout: internal/transport.defaultClientKeepaliveTimeout.
const (
	keepaliveMinPingTime          = 10 * time.Second
	defaultClientKeepaliveTimeout = 20 * time.Second
)

type Client struct {
	ctx           context.Context
	serverAddr    M.Socksaddr
	transport     *http2.Transport
	options       option.V2RayGRPCOptions
	requestHeader http.Header
	url           *url.URL
	host          string
}

func NewClient(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayGRPCOptions, tlsConfig tls.Config) adapter.V2RayClientTransport {
	var host string
	if tlsConfig != nil && tlsConfig.ServerName() != "" {
		host = M.ParseSocksaddrHostPort(tlsConfig.ServerName(), serverAddr.Port).String()
	} else {
		host = serverAddr.String()
	}
	readIdleTimeout := time.Duration(options.IdleTimeout)
	pingTimeout := time.Duration(options.PingTimeout)
	// Mirror grpc-go keepalive.ClientParameters handling in
	// grpc.WithKeepaliveParams (dialoptions.go) and the client transport
	// (internal/transport, http2_client.go):
	//   - Time is clamped to >= internal.KeepaliveMinPingTime (10s).
	//   - Timeout defaults to defaultClientKeepaliveTimeout (20s).
	if readIdleTimeout > 0 && readIdleTimeout < keepaliveMinPingTime {
		readIdleTimeout = keepaliveMinPingTime
	}
	if pingTimeout == 0 {
		pingTimeout = defaultClientKeepaliveTimeout
	}
	requestHeader := defaultClientHeader.Clone()
	if options.UserAgent != "" {
		requestHeader.Set("User-Agent", options.UserAgent)
	}
	client := &Client{
		ctx:           ctx,
		serverAddr:    serverAddr,
		options:       options,
		requestHeader: requestHeader,
		transport: &http2.Transport{
			ReadIdleTimeout:    readIdleTimeout,
			PingTimeout:        pingTimeout,
			DisableCompression: true,
		},
		url: &url.URL{
			Scheme: "https",
			Host:   serverAddr.String(),
			Path:   grpcPath(options.ServiceName),
		},
		host: host,
	}
	if tlsConfig == nil {
		client.transport.DialTLSContext = func(ctx context.Context, network, addr string, cfg *tls.STDConfig) (net.Conn, error) {
			return dialer.DialContext(ctx, network, M.ParseSocksaddr(addr))
		}
	} else {
		if len(tlsConfig.NextProtos()) == 0 {
			tlsConfig.SetNextProtos([]string{http2.NextProtoTLS})
		}
		tlsDialer := tls.NewDialer(dialer, tlsConfig)
		client.transport.DialTLSContext = func(ctx context.Context, network, addr string, cfg *tls.STDConfig) (net.Conn, error) {
			return tlsDialer.DialTLSContext(ctx, M.ParseSocksaddr(addr))
		}
	}

	return client
}

func (c *Client) DialContext(ctx context.Context) (net.Conn, error) {
	pipeInReader, pipeInWriter := io.Pipe()
	request := &http.Request{
		Method: http.MethodPost,
		Body:   pipeInReader,
		URL:    c.url,
		Header: c.requestHeader,
		Host:   c.host,
	}
	request = request.WithContext(ctx)
	conn := newLateGunConn(pipeInWriter)
	go func() {
		response, err := c.transport.RoundTrip(request)
		if err != nil {
			conn.setup(nil, err)
		} else if response.StatusCode != 200 {
			response.Body.Close()
			conn.setup(nil, E.New("v2ray-grpc: unexpected status: ", response.Status))
		} else {
			conn.setup(response.Body, nil)
		}
	}()
	return conn, nil
}

func (c *Client) Close() error {
	v2rayhttp.ResetTransport(c.transport)
	return nil
}
