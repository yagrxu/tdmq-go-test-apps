package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func init() {

}

func Setup() {

	router := gin.Default()

	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "Hello World")
	})
	router.Run(":8000")
}
