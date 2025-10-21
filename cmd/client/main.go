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
	leaders := []string{
		"leader:50051",
		"worker1:50052",
		"worker2:50053",
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		var conn *grpc.ClientConn
		var client pb.SchedulerClient

		// Try to connect
		for _, addr := range leaders {
			c, err := grpc.Dial(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithTimeout(2*time.Second),
			)
			if err != nil {
				continue
			}

			cl := pb.NewSchedulerClient(c)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = cl.ListPrimes(ctx, &pb.PrimeReq{Num: 10})
			cancel()

			if err == nil {
				conn = c
				client = cl
				break
			}
			c.Close()
		}

		if client == nil {
			log.Printf("[Client] No workers available")
			continue
		}

		// Send request
		number := int32(100)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.ListPrimes(ctx, &pb.PrimeReq{Num: number})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("[Client] Error: %v", err)
			continue
		}

		fmt.Printf("[%s] ✓ Primes from 1 to %d: %v\n✓ Processed by: %s\n\n", time.Now().Format("15:04:05"), number, resp.PrimeList, resp.ProcessedBy)
	}
}
