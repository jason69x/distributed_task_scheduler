package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"os"
	"time"

	"dist_scheduler/proto"

	"google.golang.org/grpc"
)

type WorkerInfo struct{
	Address string
	LastHeartBeat time.Time
	IsAlive bool
}
type LeaderServer struct{
	pb.UnimplementedSchedulerServer
	pb.UnimplementedLeaderRegistryServer
	pb.UnimplementedElectionServer
	workers map[string]*WorkerInfo
	workerIndex int
	workerMutex sync.RWMutex

	nodeID int32
	peers map[int32]string
	isLeader bool
	mu sync.RWMutex
}

func (s *LeaderServer) StartElection(ctx context.Context,req *pb.ElectionReq)(*pb.ElectionRes,error){
		log.Printf("[Election] Received election req from node%v",req.SenderID)
	if req.SenderID < s.nodeID{
	log.Printf("[Election] My ID (%v) is higher",s.nodeID)
	return &pb.ElectionRes{Ok: true},nil
	}
	return &pb.ElectionRes{Ok: false},nil 
}

func (s *LeaderServer) AnnounceLeader(ctx context.Context,req *pb.LeaderReq)(*pb.LeaderRes,error){
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Election] Node%v is now leader",req.LeaderID)
	s.isLeader = (req.LeaderID==s.nodeID)
	return &pb.LeaderRes{Ack: true},nil
}

func (s *LeaderServer) ListPrimes(ctx context.Context,req *pb.PrimeReq) (*pb.PrimeRes,error){
	aliveWorkers := s.getAliveWorkers()

	if len(aliveWorkers)==0{
		return nil,fmt.Errorf("no alive workers available")
	}
	s.workerMutex.Lock()
	workerAddr := aliveWorkers[s.workerIndex%len(aliveWorkers)]
	s.workerIndex++
	s.workerMutex.Unlock()

	log.Printf("assigning task to %v",workerAddr)

	conn,err := grpc.Dial(workerAddr,grpc.WithInsecure())
	if err!=nil{
		return nil,err
	}
	defer conn.Close()

	client := pb.NewWorkerClient(conn)
	return client.ListPrimes(ctx,req)
}

func (s *LeaderServer) RegisterWorker(ctx context.Context, req *pb.RegisterReq) (*pb.RegisterRes,error){
	s.workerMutex.Lock()
	defer s.workerMutex.Unlock()

	if existingWorker, exists := s.workers[req.WorkerID];exists{
		if existingWorker.IsAlive{
			log.Printf("worker %s already registered and alive",req.WorkerID)
		} else{
			log.Printf("worker %s re-joining",req.WorkerID)
			existingWorker.IsAlive= true;
			existingWorker.LastHeartBeat = time.Now()
		}
	}else{
		log.Printf("New worker registered: %s at %s", req.WorkerID, req.WorkerAddr)
		s.workers[req.WorkerID] = &WorkerInfo{
			Address: req.WorkerAddr,
			LastHeartBeat: time.Now(),
			IsAlive: true,
		}
	}
	return &pb.RegisterRes{
		Success: true,
		Msg: fmt.Sprintf("worker %s registered successfully",req.WorkerID),
	},nil
}

func (s *LeaderServer) monitorWorkers(){
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C{
		s.workerMutex.Lock()

		log.Println("=== health check ===")
		aliveCount := 0
		deadCount := 0

		for workerID,workerInfo := range s.workers{
			IsAlive := s.checkWorkerHealth(workerInfo.Address)

			if IsAlive{
				workerInfo.LastHeartBeat = time.Now()

				if !workerInfo.IsAlive{
					log.Printf("worker %s RECOVERED",workerID)
				}
				workerInfo.IsAlive =true
				aliveCount++
			} else{
				if workerInfo.IsAlive{
					log.Printf("workder %s DIED",workerID)
				}
				workerInfo.IsAlive= false
				deadCount++
			}
		}
		log.Printf("workers : %v alive, %v dead , %v total",aliveCount,deadCount,len(s.workers))
		s.workerMutex.Unlock()
	}
}

func (s *LeaderServer) checkWorkerHealth(workerAddr string) bool{
	conn,err := grpc.Dial(workerAddr,grpc.WithInsecure(),grpc.WithBlock(),grpc.WithTimeout(2*time.Second))
	if err != nil{
		return false
	}
	defer conn.Close()

	client := pb.NewWorkerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(),2*time.Second)
	defer cancel()

	_,err  = client.Heartbeat(ctx,&pb.HeartbeatReq{})
	return err==nil
}

func (s *LeaderServer) getAliveWorkers() []string{
	s.workerMutex.RLock()
	defer s.workerMutex.RUnlock()

	aliveWorkers := []string{}
	for workerID,workerInfo := range s.workers{
		if workerInfo.IsAlive{
			aliveWorkers = append(aliveWorkers, workerInfo.Address)
			log.Printf("  - %s : ALIVE",workerID)
		}
	}
	return aliveWorkers
}

func (s *LeaderServer) markWorkerDead(workerAddr string){
	s.workerMutex.Lock()
	defer s.workerMutex.Unlock()

	for workerID,WorkerInfo := range s.workers{
		if WorkerInfo.Address == workerAddr{
			WorkerInfo.IsAlive = false
			log.Printf("Marked %s as DEAD",workerID)
		}
	}
}

func main(){

	nodeIdStr := os.Getenv("NODE_ID")
	peersStr := os.Getenv("PEERS")
	nodeID,_ := strconv.Atoi(nodeIdStr)

	peers := make(map[int32]string)
	if peersStr != ""{
		for _,peer := range strings.Split(peersStr,","){
			parts := strings.Split(peer,"=")
			if len(parts) == 2{
				id, _ := strconv.Atoi(parts[0])
				peers[int32(id)] = parts[1]
			}
		}
	}

	leader := &LeaderServer{
		workers : make(map[string]*WorkerInfo),
		nodeID: int32(nodeID),
		peers: peers,
		isLeader: true,
	}

	go leader.monitorWorkers()

	lis,err := net.Listen("tcp",":50051")
	if err != nil{
		log.Fatalf("failed to listen : %v",err)
	}

	s := grpc.NewServer()
	pb.RegisterSchedulerServer(s,leader)
	pb.RegisterLeaderRegistryServer(s,leader)
	pb.RegisterElectionServer(s,leader)

	log.Println("Leader listening on port 50051")
	log.Println("waiting for workers to register...")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve : %v",err)
	}
}
