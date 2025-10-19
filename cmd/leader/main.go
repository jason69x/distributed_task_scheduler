package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"dist_scheduler/proto"
	"google.golang.org/grpc"
)

type LeaderServer struct{
	pb.UnimplementedSchedulerServer
	workers []string
	workerIndex int
	workerMutex sync.Mutex
}

func (s *LeaderServer) ListPrimes(ctx context.Context,req *pb.PrimeReq) (*pb.PrimeRes,error){
	s.workerMutex.Lock()
	if len(s.workers) == 0{
		s.workerMutex.Unlock()
		return nil,fmt.Errorf("no workers available")
	}

	workerAddr := s.workers[s.workerIndex]
	s.workerIndex = (s.workerIndex+1)%len(s.workers)
	s.workerMutex.Unlock()

	log.Printf("Forwarding request to %s",workerAddr)

	conn,err := grpc.Dial(workerAddr,grpc.WithInsecure())
	if err != nil{
		return nil,err
	}
	defer conn.Close()
	client := pb.NewWorkerClient(conn)
	return client.ListPrimes(ctx,req)
}

func (s *LeaderServer) monitorWorkers(){
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C{
		s.workerMutex.Lock()
		aliveWorkers := []string{}

		for _,workerAddr := range s.workers{
			if s.isWorkerAlive(workerAddr){
				aliveWorkers = append(aliveWorkers,workerAddr)
				log.Printf("workder %s is alive",workerAddr)
			}else {
				log.Printf("workder %s is DEAD",workerAddr)
			}
		}
		s.workers = aliveWorkers
		s.workerMutex.Unlock()
	}
}

func (s *LeaderServer) isWorkerAlive(workerAddr string) bool {
	conn,err := grpc.Dial(workerAddr,grpc.WithInsecure())
	if err != nil{
		return false
	}
	defer conn.Close()
	
	client := pb.NewWorkerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(),2*time.Second)
	defer cancel()
	_,err = client.Heartbeat(ctx,&pb.HeartbeatReq{})
	return err==nil
}

func main(){
	workers := []string{
		"worker1:50052","worker2:50053","worker3:50054"
	}

	leader := &LeaderServer{
		workers : workers,
	}

	go leader.monitorWorkers()

	lis,err := net.Listen("tcp",":50051")
	if err != nil{
		log.Fatalf("failed to listen : %v",err)
	}

	s := grpc.NewServer()
	pb.RegisterSchedulerServer(s,leader)

	log.Println("Leader listening on port 50051")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve : %v",err)
	}
}
