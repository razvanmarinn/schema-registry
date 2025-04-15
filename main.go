package main

import (
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/schema-registry/internal/handlers"

	"github.com/razvanmarinn/schema-registry/internal/db"
)

func main() {
	database, err := db.Connect_to_db()

	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()
	identity_service_cnn, err := grpc.Dial("localhost:50056", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	wc := pb.NewVerificationServiceClient(identity_service_cnn)
	r := handlers.SetupRouter(database, wc)
	r.Run(":8081")
}
