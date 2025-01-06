package router

import (
	"context"
	"net/http"
	"os"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/gin-gonic/gin"
	"k8s.io/client-go/kubernetes"

	"github.com/raefon/kuber/environment"
	"github.com/raefon/kuber/router/middleware"
	"github.com/raefon/kuber/server"
	"github.com/raefon/kuber/server/snapshot"

	snapshotclientset "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
)

// postServerBackup performs a backup against a given server instance using the
// provided backup adapter.
func postServerBackup(c *gin.Context) {
	s := middleware.ExtractServer(c)
	client := middleware.ExtractApiClient(c)
	logger := middleware.ExtractLogger(c)
	var data struct {
		Adapter snapshot.AdapterType `json:"adapter"`
		Uuid    string               `json:"uuid"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}

	config, _, _ := environment.Cluster()

	snapshotClient, err := snapshotclientset.NewForConfig(config)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	adapter := snapshot.NewLocal(snapshotClient, client, clientset, data.Uuid)

	// Attach the server ID and the request ID to the adapter log context for easier
	// parsing in the logs.
	adapter.WithLogContext(map[string]interface{}{
		"server":     s.ID(),
		"request_id": c.GetString("request_id"),
	})

	go func(b snapshot.BackupInterface, s *server.Server, logger *log.Entry) {
		if err := s.Snapshot(b); err != nil {
			logger.WithField("error", errors.WithStackIf(err)).Error("router: failed to generate server snapshot")
		}
	}(adapter, s, logger)

	c.Status(http.StatusAccepted)
}

// postServerRestoreBackup handles restoring a snapshot for a server by downloading
// or finding the given snapshot on the system and then unpacking the archive into
// the server's data directory.
//
// This endpoint will block until the snapshot is fully restored allowing for a
// spinner to be displayed in the Panel UI effectively.
//
// TODO: stop the server if it is running
func postServerRestoreBackup(c *gin.Context) {
	s := middleware.ExtractServer(c)
	client := middleware.ExtractApiClient(c)
	logger := middleware.ExtractLogger(c)

	var data struct {
		Adapter snapshot.AdapterType `binding:"required,oneof=kuber s3" json:"adapter"`
		// A UUID is always required for this endpoint, however the download URL
		// is only present when the given adapter type is s3.
		DownloadUrl string `json:"download_url"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}

	s.SetRestoring(true)
	hasError := true
	defer func() {
		if !hasError {
			return
		}

		s.SetRestoring(false)
	}()

	logger.Info("processing server snapshot restore request")

	// Now that we've cleaned up the data directory if necessary, grab the snapshot file
	// and attempt to restore it into the server directory.
	if data.Adapter == snapshot.LocalBackupAdapter {
		config, _, _ := environment.Cluster()

		snapshotClient, err := snapshotclientset.NewForConfig(config)
		if err != nil {
			middleware.CaptureAndAbort(c, err)
			return
		}

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			middleware.CaptureAndAbort(c, err)
			return
		}

		b, err := snapshot.LocateLocal(snapshotClient, client, clientset, c.Param("snapshot"))
		if err != nil {
			middleware.CaptureAndAbort(c, err)
			return
		}
		go func(s *server.Server, b snapshot.BackupInterface, logger *log.Entry) {
			logger.Info("starting restoration process for server snapshot")
			if err := s.RestoreBackup(b, nil); err != nil {
				logger.WithField("error", err).Error("failed to restore local snapshot to server")
			}
			s.Events().Publish(server.DaemonMessageEvent, "Completed server restoration from local backup.")
			s.Events().Publish(server.BackupRestoreCompletedEvent, "")
			logger.Info("completed server restoration from local snapshot")
			s.SetRestoring(false)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s.Environment.CreateSFTP(ctx, cancel)
		}(s, b, logger)
		hasError = false
		c.Status(http.StatusAccepted)
		return
	}
}

// deleteServerBackup deletes a local snapshot of a server. If the snapshot is not
// found on the machine just return a 404 error. The service calling this
// endpoint can make its own decisions as to how it wants to handle that
// response.
func deleteServerBackup(c *gin.Context) {
	config, _, _ := environment.Cluster()

	snapshotClient, err := snapshotclientset.NewForConfig(config)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	b, err := snapshot.LocateLocal(snapshotClient, middleware.ExtractApiClient(c), clientset, c.Param("snapshot"))
	if err != nil {
		// Just return from the function at this point if the backup was not located.
		if errors.Is(err, os.ErrNotExist) {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error": "The requested backup was not found on this server.",
			})
			return
		}
		middleware.CaptureAndAbort(c, err)
		return
	}
	// I'm not entirely sure how likely this is to happen, however if we did manage to
	// locate the backup previously and it is now missing when we go to delete, just
	// treat it as having been successful, rather than returning a 404.
	if err := b.Remove(); err != nil && !errors.Is(err, os.ErrNotExist) {
		middleware.CaptureAndAbort(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
