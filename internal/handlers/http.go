package handlers

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"time"

	pb "github.com/razvanmarinn/datalake/protobuf"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/schema-registry/internal/db"
	"github.com/razvanmarinn/schema-registry/internal/models"
)

type CreateSchemaBody struct {
	SchemaName string         `json:"schema_name" binding:"required"`
	Fields     []models.Field `json:"fields" binding:"required"`
	Type       string         `json:"type" binding:"required"`
}

func CheckProjectExists(client pb.VerificationServiceClient, projectName string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.VerifyProjectExistenceRequest{
		ProjectName: projectName,
	}

	resp, err := client.VerifyProjectExistence(ctx, req)
	if err != nil {
		log.Printf("gRPC VerifyProjectExistence failed for project %s: %v", projectName, err)
		return false, err
	}

	return resp.Exists, nil
}

func SetupRouter(database *sql.DB, grpcClient pb.VerificationServiceClient) *gin.Engine {
	r := gin.Default()

	r.POST("/:project_name/schema", func(c *gin.Context) {
		var body CreateSchemaBody
		projectName := c.Param("project_name")

		checkProjectExists, err := CheckProjectExists(grpcClient, projectName)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to verify project existence: " + err.Error()})
			return
		}

		if !checkProjectExists {
			c.JSON(http.StatusNotFound, gin.H{"error": "Project not found: " + projectName})
			return
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
			return
		}
		schema := models.Schema{
			ProjectName: projectName,
			Name:        body.SchemaName,
			Fields:      body.Fields,
			Version:     1,
		}
		err = db.CreateSchema(database, schema)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create schema in database: " + err.Error()})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"message": "Schema created successfully"})
	})

	r.PUT("/:project_name/schema", func(c *gin.Context) {
		var body CreateSchemaBody
		projectName := c.Param("project_name")
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		schema := models.Schema{
			ProjectName: projectName,
			Name:        body.SchemaName,
			Fields:      body.Fields,
		}
		err := db.UpdateSchema(database, schema)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "Schema created successfully"})
	})

	r.GET("/:project_name/schema/:schema_name", func(c *gin.Context) {
		projectName := c.Param("project_name")
		schemaName := c.Param("schema_name")
		schema, err := db.GetSchema(database, projectName, schemaName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, schema)
	})

	return r
}
