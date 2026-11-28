package autoscaler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// GuestConfig mirrors Fly Machines guest hardware configuration.
type GuestConfig struct {
	CPUKind  string `json:"cpu_kind"`
	CPUs     int    `json:"cpus"`
	MemoryMB int    `json:"memory_mb"`
}

// MountConfig mirrors Fly Machines volume mount configuration.
type MountConfig struct {
	Volume string `json:"volume"`
	Path   string `json:"path"`
}

// ServicePort mirrors Fly Machines service port configuration.
type ServicePort struct {
	Port     int      `json:"port,omitempty"`
	Handlers []string `json:"handlers,omitempty"`
}

// ServiceCheck mirrors Fly Machines service check configuration.
type ServiceCheck struct {
	Type        string `json:"type,omitempty"`
	Interval    string `json:"interval,omitempty"`
	Timeout     string `json:"timeout,omitempty"`
	GracePeriod string `json:"grace_period,omitempty"`
}

// ServiceConfig mirrors Fly Machines service configuration.
type ServiceConfig struct {
	Protocol     string         `json:"protocol,omitempty"`
	InternalPort int            `json:"internal_port,omitempty"`
	AutoStop     any            `json:"autostop,omitempty"`
	AutoStart    *bool          `json:"autostart,omitempty"`
	Ports        []ServicePort  `json:"ports,omitempty"`
	Checks       []ServiceCheck `json:"checks,omitempty"`
}

// FileConfig mirrors Fly Machines secret/file injection configuration.
type FileConfig struct {
	GuestPath  string `json:"guest_path"`
	SecretName string `json:"secret_name,omitempty"`
	RawValue   string `json:"raw_value,omitempty"`
}

// MachineConfig mirrors the config object of a Fly Machine.
type MachineConfig struct {
	Image                   string            `json:"image"`
	Env                     map[string]string `json:"env,omitempty"`
	Guest                   GuestConfig       `json:"guest"`
	Mounts                  []MountConfig     `json:"mounts,omitempty"`
	Services                []ServiceConfig   `json:"services,omitempty"`
	Files                   []FileConfig      `json:"files,omitempty"`
	Metadata                map[string]string `json:"metadata,omitempty"`
	SkipServiceRegistration bool              `json:"skip_service_registration"`
}

// Machine represents a Fly Machine returned by the Fly Machines API.
type Machine struct {
	ID        string        `json:"id"`
	Region    string        `json:"region"`
	State     string        `json:"state"`
	PrivateIP string        `json:"private_ip"`
	CreatedAt string        `json:"created_at"`
	Config    MachineConfig `json:"config"`
}

// Volume represents a Fly Volume returned by the Fly Machines API.
type Volume struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Region            string `json:"region"`
	SizeGB            int    `json:"size_gb"`
	AttachedMachineID string `json:"attached_machine_id"`
}

// CreateVolumeRequest is the payload for POST /apps/{app}/volumes.
type CreateVolumeRequest struct {
	Name      string `json:"name"`
	Region    string `json:"region"`
	SizeGB    int    `json:"size_gb"`
	Encrypted bool   `json:"encrypted"`
}

// CreateMachineRequest is the payload for POST /apps/{app}/machines.
type CreateMachineRequest struct {
	Region     string        `json:"region"`
	SkipLaunch bool          `json:"skip_launch"`
	Config     MachineConfig `json:"config"`
}

// UpdateMachineRequest is the payload for POST /apps/{app}/machines/{id}.
type UpdateMachineRequest struct {
	Config MachineConfig `json:"config"`
}

// FlyClient abstracts Fly Machines and Volumes API calls.
type FlyClient interface {
	ListMachines(ctx context.Context) ([]Machine, error)
	GetMachine(ctx context.Context, id string) (*Machine, error)
	CreateMachine(ctx context.Context, req CreateMachineRequest) (*Machine, error)
	UpdateMachine(ctx context.Context, id string, cfg MachineConfig) (*Machine, error)
	StopMachine(ctx context.Context, id string) error
	DeleteMachine(ctx context.Context, id string) error
	ListVolumes(ctx context.Context) ([]Volume, error)
	CreateVolume(ctx context.Context, req CreateVolumeRequest) (*Volume, error)
}

// HTTPFlyClient implements FlyClient against the Fly Machines REST API.
type HTTPFlyClient struct {
	baseURL    string
	appName    string
	token      string
	httpClient *http.Client
}

// NewHTTPFlyClient constructs an HTTPFlyClient.
func NewHTTPFlyClient(baseURL, appName, token string, timeout time.Duration) *HTTPFlyClient {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &HTTPFlyClient{
		baseURL: baseURL,
		appName: appName,
		token:   token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *HTTPFlyClient) do(ctx context.Context, method, path string, reqBody any, out any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal fly request: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}
	fullURL := fmt.Sprintf("%s/apps/%s%s", c.baseURL, url.PathEscape(c.appName), path)
	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return fmt.Errorf("build fly request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("fly API %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read fly API response %s %s: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("fly API %s %s failed (%d): %s", method, path, resp.StatusCode, string(respBytes))
	}
	if out != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, out); err != nil {
			return fmt.Errorf("decode fly API response %s %s: %w", method, path, err)
		}
	}
	return nil
}

func (c *HTTPFlyClient) ListMachines(ctx context.Context) ([]Machine, error) {
	var machines []Machine
	if err := c.do(ctx, http.MethodGet, "/machines", nil, &machines); err != nil {
		return nil, err
	}
	return machines, nil
}

func (c *HTTPFlyClient) GetMachine(ctx context.Context, id string) (*Machine, error) {
	var m Machine
	if err := c.do(ctx, http.MethodGet, "/machines/"+url.PathEscape(id), nil, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (c *HTTPFlyClient) CreateMachine(ctx context.Context, req CreateMachineRequest) (*Machine, error) {
	var m Machine
	if err := c.do(ctx, http.MethodPost, "/machines", req, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (c *HTTPFlyClient) UpdateMachine(ctx context.Context, id string, cfg MachineConfig) (*Machine, error) {
	var m Machine
	if err := c.do(ctx, http.MethodPost, "/machines/"+url.PathEscape(id), UpdateMachineRequest{Config: cfg}, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (c *HTTPFlyClient) StopMachine(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/machines/"+url.PathEscape(id)+"/stop", nil, nil)
}

func (c *HTTPFlyClient) DeleteMachine(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/machines/"+url.PathEscape(id), nil, nil)
}

func (c *HTTPFlyClient) ListVolumes(ctx context.Context) ([]Volume, error) {
	var vols []Volume
	if err := c.do(ctx, http.MethodGet, "/volumes", nil, &vols); err != nil {
		return nil, err
	}
	return vols, nil
}

func (c *HTTPFlyClient) CreateVolume(ctx context.Context, req CreateVolumeRequest) (*Volume, error) {
	var v Volume
	if err := c.do(ctx, http.MethodPost, "/volumes", req, &v); err != nil {
		return nil, err
	}
	return &v, nil
}
