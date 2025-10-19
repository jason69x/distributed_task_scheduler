package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"
	"dist_scheduler/proto"
	"google.golang.org/grpc"
)

type WorkerServer struct {
	pb.UnimplementedWorkerServer
	workerID string
}

func (s *WorkerServer) Heartbeat(ctx context.Context,req *pb.HeartbeatReq) (*pb.HeartbeatRes,error){
	return &pb.HeartbeatRes{IsAlive: true},nil
}

func (s *WorkerServer) ListPrimes(ctx context.Context,req *pb.PrimeReq) (*pb.PrimeRes,error){
	primes := []int32{}
	var i,j int32
	for i=2;i<=req.Num;i++{
		var flag bool = false
		for j=2;j*j<i;j++{
			if i%j==0{
				flag = true
				break
			}
		}
		if !flag{
			primes = append(primes,i)
		}
	}

	return &pb.PrimeRes{PrimeList: primes},nil
} 	 

func main(){
	workerID := os.Getenv("WORKER_ID")
	port := os.Getenv("PORT")

	lis,err := net.Listen("tcp",":"+port)
	if err != nil{
		log.Fatalf("failed to listen : %v",err);
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s,&WorkerServer{workerID: workerID})

	log.Printf("worker %s listening on port %s",workerID,port)

	if err:=s.Serve(lis); err!=nil{
		log.Fatalf("Failed to serve : %v",err)
	}
}
