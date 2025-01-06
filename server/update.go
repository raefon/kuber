package server

import (
	"time"

	kubernetes "github.com/raefon/kuber/environment/kubernetes"

	"github.com/raefon/kuber/environment"
)

// SyncWithEnvironment updates the environment for the server to match any of
// the changed data. This pushes new settings and environment variables to the
// environment. In addition, the in-situ update method is called on the
// environment which will allow environments that make use of it (such as Docker)
// to immediately apply some settings without having to wait on a server to
// restart.
//
// This functionality allows a server's resources limits to be modified on the
// fly and have them apply right away allowing for dynamic resource allocation
// and responses to abusive server processes.
func (s *Server) SyncWithEnvironment() {
	s.Log().Debug("syncing server settings with environment")

	cfg := s.Config()

	// Update the environment settings using the new information from this server.
	s.Environment.Config().SetSettings(environment.Settings{
		Mounts:             s.Mounts(),
		Ports:              cfg.Ports,
		Allocations:        cfg.Allocations,
		Limits:             cfg.Build,
		ConfigurationFiles: s.ProcessConfiguration().ConfigurationFiles,
		NodeSelectors:      cfg.NodeSelectors,
		StorageClass:       cfg.StorageClass,
	})

	// For Kubernetes specific environments we also want to update the configured image
	// and stop configuration.
	if e, ok := s.Environment.(*kubernetes.Environment); ok {
		s.Log().Debug("syncing stop configuration with configured kubernetes environment")
		e.SetImage(cfg.Container.Image)
		e.SetStopConfiguration(s.ProcessConfiguration().Stop)
	}

	// If build limits are changed, environment variables also change. Plus, any modifications to
	// the startup command also need to be properly propagated to this environment.
	//
	// @see https://github.com/pterodactyl/panel/issues/2255
	s.Environment.Config().SetEnvironmentVariables(s.GetEnvironmentVariables())

	if !s.IsSuspended() {
		// Update the environment in place, allowing memory and CPU usage to be adjusted
		// on the fly without the user needing to reboot (theoretically).
		s.Log().Info("checking server storage limit modification")
		if err := s.Environment.InSituUpdate(); err != nil {
			// This is not a failure, the process is still running fine and will fix itself on the
			// next boot, or fail out entirely in a more logical position.
			s.Log().WithField("error", err).Warn("failed to update server storage")
		}
	} else {
		// Checks if the server is now in a suspended state. If so and a server process is currently running it
		// will be gracefully stopped (and terminated if it refuses to stop).
		if s.Environment.State() != environment.ProcessOfflineState {
			s.Log().Info("server suspended with running process state, terminating now")

			go func(s *Server) {
				if err := s.Environment.WaitForStop(s.Context(), time.Minute, true); err != nil {
					s.Log().WithField("error", err).Warn("failed to terminate server environment after suspension")
				}
			}(s)
		}
	}
}
