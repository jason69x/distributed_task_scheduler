package main

import (
	"context"
	"dist_scheduler/proto"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type WorkerServer struct {
	pb.UnimplementedWorkerServer
	pb.UnimplementedElectionServer
	workerID string
	workerAddr string
	leaderAddr string

	nodeID int32
	peers map[int32]string
	isLeader bool
	mu sync.RWMutex
}

func (s *WorkerServer) StartElection(ctx context.Context,req *pb.ElectionReq)(*pb.ElectionRes,error){
	log.Printf("[Election] received election request from node %d",req.SenderID)

	if req.SenderID < s.nodeID{
		log.Printf("[Election] my id (%v) is higher. starting my own election",s.nodeID)
		go s.runElection()
		return &pb.ElectionRes{Ok: true},nil
	}

	return &pb.ElectionRes{Ok: false},nil
}

func (s *WorkerServer) AnnounceLeader(ctx context.Context,req *pb.LeaderReq)(*pb.LeaderRes,error){
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Election] Node%v is the new leader",req.LeaderID)
	s.isLeader = (req.LeaderID==s.nodeID)

	if s.isLeader{
		log.Printf("[Election] i am the new leader!,surrender you peasants hahaha")
	}
	return &pb.LeaderRes{Ack: true},nil
}


func (s *WorkerServer) runElection(){
	log.Printf("[Election] starting election (my Id : %d)",s.nodeID)

	higherNodes := []int32{}
	for peerID := range s.peers{
		if peerID > s.nodeID{
			higherNodes = append(higherNodes, peerID)
		}
	}

	if len(higherNodes) == 0{
		log.Printf("[Election] no higher nodes. i win!")
		s.becomeLeader()
		return
	}

	anyResponse := false
	for _,higherID := range higherNodes{
		peerAddr := s.peers[higherID]
		conn,err := grpc.Dial(peerAddr,grpc.WithInsecure(),grpc.WithTimeout(2*time.Second))
		if err != nil{
			continue
		}

		client := pb.NewElectionClient(conn)
		ctx,cancel := context.WithTimeout(context.Background(),2*time.Second)
		resp,err := client.StartElection(ctx, &pb.ElectionReq{SenderID: s.nodeID})
		cancel()
		conn.Close()

		if err == nil && resp.Ok{
			anyResponse = true
			log.Printf("[Election] Node%d responded",higherID)
		}
	}

	if !anyResponse{
		log.Printf("[Election] No responses. I win!")
		s.becomeLeader()
	}
}

func (s *WorkerServer) becomeLeader(){
	s.mu.Lock()
	s.isLeader = true
	s.mu.Unlock()

	log.Printf("[Election] i am the leader")

	for peerID,peerAddr := range s.peers{
		go func(id int32, addr string){
			conn,err:= grpc.Dial(addr,grpc.WithInsecure(),grpc.WithTimeout(2*time.Second))
			if err != nil {
				return
			}
			defer conn.Close()

			client := pb.NewElectionClient(conn)
			ctx,cancel := context.WithTimeout(context.Background(),2*time.Second)
			defer cancel()

			client.AnnounceLeader(ctx,&pb.LeaderReq{LeaderID: s.nodeID})
			log.Printf("[Election] Announced leadership to node%d",id)
		}(peerID,peerAddr)
	}
}

func (s *WorkerServer) checkLeaderHealth(){
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C{
		s.mu.RLock()
		isLeader := s.isLeader
		s.mu.RUnlock()

		if isLeader{
			continue
		}

		conn,err := grpc.Dial(s.leaderAddr,grpc.WithInsecure(),grpc.WithTimeout(2*time.Second))
		if err != nil {
			log.Printf("[Monitor] Leader is dead! Starting Election...")
			go s.runElection()
			continue
		}

		client := pb.NewWorkerClient(conn)
		ctx,cancel := context.WithTimeout(context.Background(),2*time.Second)
		_,err = client.Heartbeat(ctx,&pb.HeartbeatReq{})
		cancel()
		conn.Close()

		if err!= nil{
			log.Printf("[Monitor] Leader is dead! Starting Election...")
			go s.runElection()
		}
	}
}

func (s *WorkerServer) registerWithLeader() error {
    log.Printf("Attempting to register with leader at %s", s.leaderAddr)
    
    conn, err := grpc.Dial(s.leaderAddr, 
        grpc.WithInsecure(),
        grpc.WithBlock(),
        grpc.WithTimeout(5*time.Second))
    if err != nil {
        return fmt.Errorf("failed to connect to leader: %v", err)
    }
    defer conn.Close()
    
    client := pb.NewLeaderRegistryClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    res, err := client.RegisterWorker(ctx, &pb.RegisterReq{
        WorkerID:   s.workerID,
        WorkerAddr: s.workerAddr,
    })
    
    if err != nil {
        return fmt.Errorf("registration failed: %v", err)
    }
    
    log.Printf("✅ Registration successful: %s", res.Msg)
    return nil
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

	return &pb.PrimeRes{PrimeList: primes,ProcessedBy: s.workerID,},nil
	
} 	 

func main(){
	workerID := os.Getenv("WORKER_ID")
	port := os.Getenv("PORT")
	leaderAddr := os.Getenv("LEADER_ADDR")
	peersStr := os.Getenv("PEERS")
	nodeIDStr := os.Getenv("NODE_ID")

	if leaderAddr == ""{
		leaderAddr = "leader:50051"
	}
	nodeID, _ := strconv.Atoi(nodeIDStr)

	peers := make(map[int32]string)
	if peersStr !=""{
		for _,peer := range strings.Split(peersStr,","){
			parts := strings.Split(peer,"=")
			if len(parts) == 2{
				id, _ := strconv.Atoi(parts[0])
				peers[int32(id)] = parts[1]
			}
		}
	}
	workerAddr := fmt.Sprintf("%s:%s",workerID,port)

	worker := &WorkerServer{
		workerID : workerID,
		workerAddr: workerAddr,
		leaderAddr: leaderAddr,
		nodeID: int32(nodeID),
		peers: peers,
		isLeader: false,
	}
	go worker.checkLeaderHealth()
	lis,err := net.Listen("tcp",":"+port)
	if err != nil{
		log.Fatalf("failed to listen : %v",err);
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s,worker)
	pb.RegisterElectionServer(s,worker)

	go func(){
		time.Sleep(2 *time.Second)
		for{
			err := worker.registerWithLeader()
			if err == nil{
				log.Printf("successfully registered with leader")
				break
			}
			log.Printf("Registration failed, retrying in 2s: %v",err)
			time.Sleep(2*time.Second)
		}
	}()

	log.Printf("worker %s listening on port %s",workerID,port)

	if err:=s.Serve(lis); err!=nil{
		log.Fatalf("Failed to serve : %v",err)
	}
}
