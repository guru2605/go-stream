package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
)

// Record represents the structure of each record
type Record struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	// Add other fields as needed
}

// insertBatch simulates the database insertion for a batch of records
func insertBatch(records []Record) error {
	// Your database insertion logic goes here
	// Replace this with your actual database operation
	fmt.Printf("Inserting %d records into the database\n", len(records))
	return nil
}

// processJSONStream processes a JSON stream in batches
func processJSONStream(c *gin.Context) {
	decoder := json.NewDecoder(c.Request.Body)

	// Start decoding the JSON array
	_, err := decoder.Token()
	if err != nil {
		c.JSON(500, gin.H{"error": "Error decoding JSON stream"})
		return
	}
	batchSize := 100
	// Loop through the array elements
	for decoder.More() {
		var batch []Record
		for i := 0; i < batchSize; i++ {
			var record Record
			err := decoder.Decode(&record)
			if err != nil {
				c.JSON(500, gin.H{"error": "Error decoding JSON stream"})
				break
				return
			}
			batch = append(batch, record)
		}

		// Process the record or add it to a batch
		err = insertBatch(batch)
		if err != nil {
			c.JSON(500, gin.H{"error": "Error inserting records into the database"})
			return
		}
		batch = []Record{}

		// Optionally, you can log or return information about the processed record
		// fmt.Printf("Processed record %+v\n", batch)
	}

	// End decoding the JSON array
	_, err = decoder.Token()
	if err != nil {
		c.JSON(500, gin.H{"error": "Error decoding JSON stream"})
		return
	}

	c.JSON(200, gin.H{"message": "JSON stream processed successfully"})

}

func main() {
	r := gin.New()

	// Endpoint for processing JSON stream
	r.POST("/processJSONStream", processJSONStream)

	// Run the server on port 8080
	if err := r.Run(":8081"); err != nil {
		log.Fatal("Error starting Gin server:", err)
	}
}
