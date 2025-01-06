package router

import (
	"bufio"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/raefon/kuber/router/middleware"
	"github.com/raefon/kuber/router/tokens"
)

// Handles downloading a specific file for a server.
func getDownloadFile(c *gin.Context) {
	manager := middleware.ExtractManager(c)
	token := tokens.FilePayload{}
	if err := tokens.ParseToken([]byte(c.Query("token")), &token); err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	s, ok := manager.Get(token.ServerUuid)
	if !ok || !token.IsUniqueRequest() {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "The requested resource was not found on this server.",
		})
		return
	}

	p, _ := s.Filesystem().SafePath(token.FilePath)
	st, err := s.Filesystem().Stat(p)
	// If there is an error or we're somehow trying to download a directory, just
	// respond with the appropriate error.
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	} else if st.IsDir() {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "The requested resource was not found on this server.",
		})
		return
	}

	f, _, err := s.Filesystem().File(p)
	if err != nil {
		middleware.CaptureAndAbort(c, err)
		return
	}

	c.Header("Content-Length", strconv.Itoa(int(st.Size())))
	c.Header("Content-Disposition", "attachment; filename="+strconv.Quote(st.Name()))
	c.Header("Content-Type", "application/octet-stream")

	_, _ = bufio.NewReader(f).WriteTo(c.Writer)
}
