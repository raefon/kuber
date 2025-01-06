package environment

import (
	"context"
	"regexp"
	"time"

	"github.com/raefon/kuber/events"
	corev1 "k8s.io/api/core/v1"
)

const (
	StateChangeEvent         = "state change"
	ResourceEvent            = "resources"
	DockerImagePullStarted   = "docker image pull started"
	DockerImagePullStatus    = "docker image pull status"
	DockerImagePullCompleted = "docker image pull completed"
	DockerImagePullBackOff   = "docker image pull back-off"
	DockerImagePullErr       = "docker image pull error"
)

const (
	ProcessOfflineState  = "offline"
	ProcessStartingState = "starting"
	ProcessRunningState  = "running"
	ProcessStoppingState = "stopping"
)

var LabelNameRegex = regexp.MustCompile(`^[a-zA-Z]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?(\/[a-zA-Z]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?)*$`)
var LabelValueRegex = regexp.MustCompile(`^([A-Za-z0-9][-A-Za-z0-9_.]*[A-Za-z0-9])?$`)

// Defines the basic interface that all environments need to implement so that
// a server can be properly controlled.
type ProcessEnvironment interface {
	// Returns the name of the environment.
	Type() string

	// Returns the environment configuration to the caller.
	Config() *Configuration

	// Returns an event emitter instance that can be hooked into to listen for different
	// events that are fired by the environment. This should not allow someone to publish
	// events, only subscribe to them.
	Events() *events.Bus

	// Determines if the server instance exists. Returns true or false.
	Exists() (bool, error)

	// IsRunning determines if the environment is currently active and running
	// a server process for this specific server instance.
	IsRunning(ctx context.Context) (bool, error)

	// Create unique service port number
	CreateServiceWithUniquePort() (int, error)

	// Monitor changes and events of a Kubernetes pod
	WatchPodEvents(ctx context.Context) error

	// Performs an update of server resource limits without actually stopping the server
	// process. This only executes if the environment supports it, otherwise it is
	// a no-op.
	InSituUpdate() error

	// Runs before the environment is started. If an error is returned starting will
	// not occur, otherwise proceeds as normal.
	OnBeforeStart(ctx context.Context) error

	// Starts a server instance. If the server instance is not in a state where it
	// can be started an error should be returned.
	Start(ctx context.Context) error

	// Stop stops a server instance. If the server is already stopped an error will
	// not be returned, this function will act as a no-op.
	Stop(ctx context.Context) error

	// WaitForStop waits for a server instance to stop gracefully. If the server is
	// still detected as running after "duration", an error will be returned, or the server
	// will be terminated depending on the value of the second argument. If the context
	// provided is canceled the underlying wait conditions will be stopped and the
	// entire loop will be ended (potentially without stopping or terminating).
	WaitForStop(ctx context.Context, duration time.Duration, terminate bool) error

	// Terminate stops a running server instance using the provided signal. This function
	// is a no-op if the server is already stopped.
	Terminate(ctx context.Context) error

	// Destroys the environment removing any containers that were created (in Kubernetes
	// environments at least).
	Destroy() error

	// Returns the exit state of the process. The first result is the exit code, the second
	// determines if the process was killed by the system OOM killer.
	ExitState() (uint32, bool, error)

	// Creates the pod for SFTP server.
	CreateSFTP(ctx context.Context, cancelFunc context.CancelFunc) error

	// Creates the necessary environment for running the server process. For example,
	// in the Docker environment create will create a new container instance for the
	// server.
	Create() error

	// Creates the necessary services in Kubernetes for server.
	CreateService() error

	// Attach attaches to the server console environment and allows piping the output
	// to a websocket or other internal tool to monitor output. Also allows you to later
	// send data into the environment's stdin.
	Attach(ctx context.Context) error

	// Sends the provided command to the running server instance.
	SendCommand(string) error

	// Return service details
	GetServiceDetails() []corev1.Service

	// Reads the log file for the process from the end backwards until the provided
	// number of lines is met.
	Readlog(int64) ([]string, error)

	// Returns the current state of the environment.
	State() string

	// Sets the current state of the environment. In general you should let the environment
	// handle this itself, but there are some scenarios where it is helpful for the server
	// to update the state externally (e.g. starting -> started).
	SetState(string)

	// Uptime returns the current environment uptime in milliseconds. This is
	// the time that has passed since it was last started.
	Uptime(ctx context.Context) (int64, error)

	// SetLogCallback sets the callback that the container's log output will be passed to.
	SetLogCallback(func([]byte))

	ConfigurePodSpecForRestrictedStandard(*corev1.PodSpec) error
}
