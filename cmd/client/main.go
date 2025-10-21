package main

import (
	"context"
	"dist_scheduler/proto"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Try to connect to any leader with retries
	leaders := []string{
		"leader:50051",
		"worker1:50052",
		"worker2:50053",
	}

	var conn *grpc.ClientConn
	var client pb.SchedulerClient

	// Retry logic
	maxRetries := 10
	for attempt := 1; attempt <= maxRetries; attempt++ {
		for _, addr := range leaders {
			log.Printf("Attempt %d: Trying to connect to %s...", attempt, addr)
			
			c, err := grpc.Dial(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithTimeout(3*time.Second),
			)
			if err != nil {
				log.Printf("  Cannot connect to %s: %v", addr, err)
				continue
			}

			cl := pb.NewSchedulerClient(c)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err = cl.ListPrimes(ctx, &pb.PrimeReq{Num: 10})
			cancel()

			if err == nil {
				conn = c
				client = cl
				log.Printf("✓ Connected to leader at %s", addr)
				break
			}
			log.Printf("  Leader check failed on %s: %v", addr, err)
			c.Close()
		}

		if client != nil {
			break
		}

		if attempt < maxRetries {
			log.Printf("Will retry in 2 seconds...")
			time.Sleep(2 * time.Second)
		}
	}

	if client == nil {
		log.Fatalf("Failed to connect to any leader after %d attempts", maxRetries)
	}
	defer conn.Close()

	// Send request
	number := int32(100)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := client.ListPrimes(ctx, &pb.PrimeReq{Num: number})
	cancel()

	if err != nil {
		log.Printf("Error: %v", err)
		return
	}

	fmt.Printf("\n✓ Primes from 1 to %d: %v\n✓ Processed by: %s\n", number, resp.PrimeList, resp.ProcessedBy)
}
