package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"dist_scheduler/proto"
	"google.golang.org/grpc"
)

func main(){
	conn, err := grpc.Dial("leader:50051",grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Connection failed: %v",err)
	}
	defer conn.Close()

	client := pb.NewSchedulerClient(conn)

	number := 40

	ctx,cancel := context.WithTimeout(context.Background(),5*time.Second)

	resp,err := client.ListPrimes(ctx,&pb.PrimeReq{Num: int32(number)})
	if err != nil {
		log.Printf("Error checking %v: %v",number,err)
	}else{
		fmt.Printf("primes from 1 to %v : %v\n ProcessedBy: %v",number,resp.PrimeList,resp.ProcessedBy)
	}
	cancel()
}
