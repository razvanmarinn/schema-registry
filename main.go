package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	"github.com/linkedin/goavro/v2"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/schema-registry/internal/models"

	"github.com/razvanmarinn/schema-registry/internal/db"
)

func setupRouter(database *sql.DB) *gin.Engine {
	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	r.POST("/:project_name/schema", func(c *gin.Context) {
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
			Version:     1,
		}
		err := db.CreateSchema(database, schema)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "Schema created successfully"})
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

func main() {
	database, err := db.Connect_to_db()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	r := setupRouter(database)
	r.Run(":8081")
}

type CreateSchemaBody struct {
	SchemaName string         `json:"schema_name" binding:"required"`
	Fields     []models.Field `json:"fields" binding:"required"`
	Type       string         `json:"type" binding:"required"`
}

func generateAvroSchema(schemaName string, fields []models.Field) (string, error) {
	avroFields := []map[string]interface{}{}
	for _, field := range fields {
		avroFields = append(avroFields, map[string]interface{}{
			"name": field.Name,
			"type": field.Type,
		})
	}

	avroSchema := map[string]interface{}{
		"type":      "record",
		"name":      schemaName,
		"namespace": "com.example",
		"fields":    avroFields,
	}

	schemaBytes, err := json.Marshal(avroSchema)
	if err != nil {
		return "", err
	}

	// Validate the generated Avro schema
	_, err = goavro.NewCodec(string(schemaBytes))
	if err != nil {
		return "", err
	}

	return string(schemaBytes), nil
}
