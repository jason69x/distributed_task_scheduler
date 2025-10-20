package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"dist_scheduler/proto"
	"google.golang.org/grpc"
)
func tryConnect(addresses []string) (pb.SchedulerClient,*grpc.ClientConn,error){
	for _,addr := range addresses{
		conn, err := grpc.Dial(addr,grpc.WithInsecure(),grpc.WithTimeout(2*time.Second))
		if err != nil{
			log.Printf("cannot connect to %v,trying next...",addr)
			continue
		}
		client := pb.NewSchedulerClient(conn)
		ctx,cancel := context.WithTimeout(context.Background(),2*time.Second)
		_,err = client.ListPrimes(ctx,&pb.PrimeReq{Num: 10})
		cancel()

		if err == nil{
			log.Printf(" found leader at %v",addr)
			return client,conn,nil
		}
		conn.Close()
	}
	return nil,nil,fmt.Errorf("no leader found")
}
func main(){
	addresses := []string{
		"leader:50051",
		"worker1:50052",
		"worker2:50053",
	}
	client,conn,err := tryConnect(addresses)
	if err!= nil {
		log.Fatalf("failed to find leader: %v",err)
	}
	defer conn.Close()
	
	number := int32(100)
	ctx,cancel := context.WithTimeout(context.Background(),5*time.Second)
	defer cancel()

	resp,err := client.ListPrimes(ctx,&pb.PrimeReq{Num: number})
	if err != nil {
		log.Printf("Error checking %v: %v",number,err)
		return
	}
		fmt.Printf("primes from 1 to %v : %v\n ProcessedBy: %v",number,resp.PrimeList,resp.ProcessedBy)
	
}
