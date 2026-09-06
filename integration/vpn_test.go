package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/traefik/traefik/v3/integration/try"
)

func TestWaitForVPNRouteReady(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, "Name: current-suite\n")
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- waitForVPNRoute(ctx, server.URL, "current-suite") }()
	watchdog := time.NewTimer(6 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-result:
		require.NoError(t, err)
		require.NoError(t, ctx.Err())
	case <-watchdog.C:
		t.Fatal("readiness check did not return within the independent time limit")
	}
	require.EqualValues(t, 1, requests.Load())
}

func waitForVPNRoute(ctx context.Context, endpoint, identity string) error {
	// Probe the subnet directly, even when the host uses an HTTP proxy.
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{}}
	defer client.CloseIdleConnections()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("VPN route is not ready: %w", err)
		}

		err := checkVPNRoute(ctx, client, endpoint, identity)
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("VPN route is not ready: %w (last attempt: %w)", ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

func checkVPNRoute(ctx context.Context, client *http.Client, endpoint, identity string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return fmt.Errorf("create VPN readiness request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request VPN readiness: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("VPN readiness returned status %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if scanner.Text() == "Name: "+identity {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read VPN readiness response: %w", err)
	}

	return fmt.Errorf("VPN readiness response has no identity %q", identity)
}

func TestWaitForVPNRouteWaitsForCurrentSuite(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) < 3 {
			_, _ = io.WriteString(w, "Name: previous-suite\n")
			return
		}
		_, _ = io.WriteString(w, "Name: current-suite\n")
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- waitForVPNRoute(ctx, server.URL, "current-suite") }()
	watchdog := time.NewTimer(6 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-result:
		require.NoError(t, err)
		require.NoError(t, ctx.Err())
	case <-watchdog.C:
		t.Fatal("readiness check did not return within the independent time limit")
	}
	require.EqualValues(t, 3, requests.Load())
}

func TestWaitForVPNRouteBlocksUntilIdentityResponse(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
			_, _ = io.WriteString(w, "Name: current-suite\n")
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- waitForVPNRoute(ctx, server.URL, "current-suite") }()
	watchdog := time.NewTimer(6 * time.Second)
	defer watchdog.Stop()
	select {
	case <-entered:
	case err := <-result:
		t.Fatalf("readiness returned before reaching the blocked endpoint: %v", err)
	case <-watchdog.C:
		t.Fatal("readiness did not reach the blocked endpoint")
	}
	// This is a negative observation window, not a sleep used to establish readiness.
	observation := time.NewTimer(200 * time.Millisecond)
	defer observation.Stop()
	select {
	case err := <-result:
		t.Fatalf("readiness returned before the identity response was released: %v", err)
	case <-observation.C:
	}
	close(release)
	select {
	case err := <-result:
		require.NoError(t, err)
		require.NoError(t, ctx.Err())
	case <-watchdog.C:
		t.Fatal("readiness did not return after the identity response was released")
	}
}

func TestWaitForVPNRouteRejectsWrongIdentity(t *testing.T) {
	for _, response := range []string{"Name: previous-suite\n", "Name: current-suite-old\n", "", "GET /current-suite HTTP/1.1\n"} {
		t.Run(response, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				_, _ = io.WriteString(w, response)
			}))
			defer server.Close()
			defer server.CloseClientConnections()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			result := make(chan error, 1)
			go func() { result <- waitForVPNRoute(ctx, server.URL, "current-suite") }()
			watchdog := time.NewTimer(3 * time.Second)
			defer watchdog.Stop()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
			case <-watchdog.C:
				t.Fatal("readiness check did not return within the independent time limit")
			}
			require.Positive(t, requests.Load())
		})
	}
}

func TestWaitForVPNRouteRejectsUnhealthyResponse(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "Name: current-suite\n")
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- waitForVPNRoute(ctx, server.URL, "current-suite") }()
	watchdog := time.NewTimer(3 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	case <-watchdog.C:
		t.Fatal("readiness check did not return within the independent time limit")
	}
	require.Positive(t, requests.Load())
}

func TestWaitForVPNRouteBoundsUnresponsiveServer(t *testing.T) {
	for _, stage := range []string{"headers", "body"} {
		t.Run(stage, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if stage == "body" {
					w.WriteHeader(http.StatusOK)
					_ = http.NewResponseController(w).Flush()
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			defer server.CloseClientConnections()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			result := make(chan error, 1)
			go func() { result <- waitForVPNRoute(ctx, server.URL, "current-suite") }()
			watchdog := time.NewTimer(3 * time.Second)
			defer watchdog.Stop()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
			case <-watchdog.C:
				t.Fatal("readiness check did not return within the independent time limit")
			}
			require.Positive(t, requests.Load())
		})
	}
}

func TestWaitForVPNRouteHonorsCancellation(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, "Name: current-suite\n")
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result := make(chan error, 1)
	go func() { result <- waitForVPNRoute(ctx, server.URL, "current-suite") }()
	watchdog := time.NewTimer(3 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	case <-watchdog.C:
		t.Fatal("readiness check did not return within the independent time limit")
	}
	require.Zero(t, requests.Load())
}

func TestWaitForVPNRouteBoundsConnectionFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- waitForVPNRoute(ctx, "http://"+address, "current-suite") }()
	watchdog := time.NewTimer(3 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	case <-watchdog.C:
		t.Fatal("readiness check did not return within the independent time limit")
	}
}

func TestWaitForVPNRouteRecoversFromStalledAttempt(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, "Name: current-suite\n")
	}))
	defer server.Close()
	defer server.CloseClientConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- waitForVPNRoute(ctx, server.URL, "current-suite") }()
	watchdog := time.NewTimer(6 * time.Second)
	defer watchdog.Stop()
	select {
	case err := <-result:
		require.NoError(t, err)
		require.NoError(t, ctx.Err())
	case <-watchdog.C:
		t.Fatal("readiness check did not return within the independent time limit")
	}
	require.EqualValues(t, 2, requests.Load())
}

func TestBaseSuiteWaitsForVPNRouteHandoff(t *testing.T) {
	if !isDockerDesktop(t) {
		t.Skip("the suite VPN is only used on Docker Desktop")
	}

	for _, name := range []string{"initial_suite", "replacement_suite"} {
		if !t.Run(name, func(t *testing.T) {
			s := &BaseSuite{}
			s.SetT(t)
			defer s.TearDownSuite()
			s.SetupSuite()

			s.createComposeProject("tcp_healthcheck")
			s.composeUp("whoamitcp1")
			ip := s.getComposeServiceIP("whoamitcp1")
			require.NotEmpty(t, ip)
			address := net.JoinHostPort(ip, "8080")
			hostname := s.containers["whoamitcp1"].GetContainerID()[:12]

			// Establish listener readiness without waiting for the host's VPN route.
			err := try.Do(5*time.Second, func() error {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				code, reader, err := s.vpn.Exec(ctx, []string{"sh", "-c", "printf WHO | nc -w 1 " + ip + " 8080"})
				if err != nil {
					return err
				}
				out, err := io.ReadAll(reader)
				if err != nil {
					return err
				}
				if code != 0 || !strings.Contains(string(out), hostname) {
					return fmt.Errorf("backend is not responding through its current gateway: exit %d, response %q", code, out)
				}
				return nil
			})
			require.NoError(t, err)

			if name == "initial_suite" {
				// Ensure the predecessor is carrying traffic before replacing it.
				err := try.Do(30*time.Second, func() error {
					conn, err := net.DialTimeout("tcp", address, time.Second)
					if err != nil {
						return err
					}
					return conn.Close()
				})
				require.NoError(t, err)
			}

			conn, err := net.DialTimeout("tcp", address, time.Second)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
			_, err = conn.Write([]byte("WHO"))
			require.NoError(t, err)
			scanner := bufio.NewScanner(conn)
			found := false
			for scanner.Scan() {
				if scanner.Text() == "Hostname: "+hostname {
					found = true
					break
				}
			}
			require.NoError(t, scanner.Err())
			require.True(t, found, "current backend hostname was not received")
		}) {
			return
		}
	}
}

func TestBaseSuiteCleansUpAfterSetupAbort(t *testing.T) {
	type resources struct {
		NetworkID string `json:"NetworkID"`
		VPNID     string `json:"VPNID"`
	}
	if manifest := os.Getenv("TRAEFIK_VPN_SETUP_ABORT_MANIFEST"); manifest != "" {
		s := &BaseSuite{}
		s.SetT(t)
		defer func() {
			var created resources
			if s.network != nil {
				created.NetworkID = s.network.ID
			}
			if s.vpn != nil {
				created.VPNID = s.vpn.GetContainerID()
			}
			data, err := json.Marshal(created)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(manifest, data, 0o600))
		}()
		s.SetupSuite()
		// An enclosing SetupSuite can fail before testify registers TearDownSuite.
		t.Fatal("intentional VPN setup abort")
	}
	if !isDockerDesktop(t) {
		t.Skip("the suite VPN is only used on Docker Desktop")
	}

	manifest := filepath.Join(t.TempDir(), "resources.json")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBaseSuiteCleansUpAfterSetupAbort$", "-test.timeout=100s")
	cmd.Env = append(os.Environ(), "TRAEFIK_VPN_SETUP_ABORT_MANIFEST="+manifest, "TESTCONTAINERS_RYUK_DISABLED=true")
	output, runErr := cmd.CombinedOutput()

	data, err := os.ReadFile(manifest)
	require.NoError(t, err)
	var created resources
	require.NoError(t, json.Unmarshal(data, &created))
	require.NotEmpty(t, created.NetworkID)
	cli, err := testcontainers.NewDockerClientWithOpts(t.Context())
	require.NoError(t, err)
	defer cli.Close()
	// The parent is a safety net only; leak assertions run before this cleanup.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cleanupCancel()
		remaining, err := cli.ContainerList(cleanupCtx, client.ContainerListOptions{
			All: true, Filters: make(client.Filters).Add("network", created.NetworkID),
		})
		require.NoError(t, err)
		for _, con := range remaining.Items {
			_, err := cli.ContainerRemove(cleanupCtx, con.ID, client.ContainerRemoveOptions{Force: true})
			require.NoError(t, err)
		}
		networks, err := cli.NetworkList(cleanupCtx, client.NetworkListOptions{Filters: make(client.Filters).Add("id", created.NetworkID)})
		require.NoError(t, err)
		for _, network := range networks.Items {
			_, err := cli.NetworkRemove(cleanupCtx, network.ID, client.NetworkRemoveOptions{})
			require.NoError(t, err)
		}
	}()

	require.Error(t, runErr)
	require.Contains(t, string(output), "intentional VPN setup abort")
	require.NoError(t, ctx.Err())
	require.NotEmpty(t, created.VPNID)
	containers, err := cli.ContainerList(t.Context(), client.ContainerListOptions{
		All: true, Filters: make(client.Filters).Add("network", created.NetworkID),
	})
	require.NoError(t, err)
	vpn, err := cli.ContainerList(t.Context(), client.ContainerListOptions{
		All: true, Filters: make(client.Filters).Add("id", created.VPNID),
	})
	require.NoError(t, err)
	networks, err := cli.NetworkList(t.Context(), client.NetworkListOptions{Filters: make(client.Filters).Add("id", created.NetworkID)})
	require.NoError(t, err)
	assert.Empty(t, containers.Items, "suite network still has containers after setup abort")
	assert.Empty(t, vpn.Items, "suite VPN remains after setup abort")
	assert.Empty(t, networks.Items, "suite network remains after setup abort")
}
