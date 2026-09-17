package v2raygrpclite

import (
	"context"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2rayhttp"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	aTLS "github.com/sagernet/sing/common/tls"
	sHttp "github.com/sagernet/sing/protocol/http"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c" //nolint:staticcheck
)

var _ adapter.V2RayServerTransport = (*Server)(nil)

// defaultServerKeepaliveTimeout mirrors internal/transport.defaultServerKeepaliveTimeout
// in google.golang.org/grpc, applied when keepalive Timeout is unset.
const defaultServerKeepaliveTimeout = 20 * time.Second

type Server struct {
	tlsConfig  tls.ServerConfig
	logger     logger.ContextLogger
	handler    adapter.V2RayServerTransportHandler
	httpServer *http.Server
	h2Server   *http2.Server
	h2cHandler http.Handler
	path       string
}

func NewServer(ctx context.Context, logger logger.ContextLogger, options option.V2RayGRPCOptions, tlsConfig tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (*Server, error) {
	readIdleTimeout := time.Duration(options.IdleTimeout)
	pingTimeout := time.Duration(options.PingTimeout)
	// Mirror grpc-go keepalive.ServerParameters handling in grpc.KeepaliveParams
	// (server.go) and the server transport (internal/transport, http2_server.go):
	//   - Time is clamped to >= internal.KeepaliveMinServerPingTime (1s).
	//   - Timeout defaults to defaultServerKeepaliveTimeout (20s).
	if readIdleTimeout > 0 && readIdleTimeout < time.Second {
		readIdleTimeout = time.Second
	}
	if pingTimeout == 0 {
		pingTimeout = defaultServerKeepaliveTimeout
	}
	server := &Server{
		tlsConfig: tlsConfig,
		logger:    logger,
		handler:   handler,
		path:      grpcPath(options.ServiceName),
		h2Server: &http2.Server{
			ReadIdleTimeout: readIdleTimeout,
			PingTimeout:     pingTimeout,
		},
	}
	server.httpServer = &http.Server{
		Handler: server,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return log.ContextWithNewID(ctx)
		},
	}
	//nolint:staticcheck
	server.h2cHandler = h2c.NewHandler(server, server.h2Server)
	if h2Err := http2.ConfigureServer(server.httpServer, server.h2Server); h2Err != nil {
		return nil, h2Err
	}
	return server, nil
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method == "PRI" && len(request.Header) == 0 && request.URL.Path == "*" && request.Proto == "HTTP/2.0" {
		s.h2cHandler.ServeHTTP(writer, request)
		return
	}
	if request.URL.Path != s.path {
		s.invalidRequest(writer, request, http.StatusNotFound, E.New("bad path: ", request.URL.Path))
		return
	}
	if request.Method != http.MethodPost {
		s.invalidRequest(writer, request, http.StatusNotFound, E.New("bad method: ", request.Method))
		return
	}
	if ct := request.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/grpc") {
		s.invalidRequest(writer, request, http.StatusNotFound, E.New("bad content type: ", ct))
		return
	}
	writer.Header().Set("Content-Type", "application/grpc")
	writer.Header().Set("TE", "trailers")
	writer.WriteHeader(http.StatusOK)
	done := make(chan struct{})
	conn := v2rayhttp.NewHTTP2Wrapper(newGunConn(request.Body, writer, writer.(http.Flusher)))
	s.handler.NewConnectionEx(request.Context(), conn, sHttp.SourceAddress(request), M.Socksaddr{}, N.OnceClose(func(it error) {
		close(done)
	}))
	<-done
	conn.CloseWrapper()
}

func (s *Server) invalidRequest(writer http.ResponseWriter, request *http.Request, statusCode int, err error) {
	if statusCode > 0 {
		writer.WriteHeader(statusCode)
	}
	s.logger.ErrorContext(request.Context(), E.Cause(err, "process connection from ", request.RemoteAddr))
}

func (s *Server) Network() []string {
	return []string{N.NetworkTCP}
}

func (s *Server) Serve(listener net.Listener) error {
	if s.tlsConfig != nil {
		if !common.Contains(s.tlsConfig.NextProtos(), http2.NextProtoTLS) {
			s.tlsConfig.SetNextProtos(append([]string{"h2"}, s.tlsConfig.NextProtos()...))
		}
		listener = aTLS.NewListener(listener, s.tlsConfig)
	}
	return s.httpServer.Serve(listener)
}

func (s *Server) ServePacket(listener net.PacketConn) error {
	return os.ErrInvalid
}

func (s *Server) Close() error {
	return common.Close(common.PtrOrNil(s.httpServer))
}
