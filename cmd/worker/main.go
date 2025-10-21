package main

import (
	"context"
	"dist_scheduler/proto"
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
	pb.UnimplementedSchedulerServer

	workerID string
	nodeID   int32
	port     string
	peers    map[int32]string // map of nodeID -> address

	mu              sync.RWMutex
	currentLeaderID int32
	isLeader        bool
	inElection      bool
}

// Heartbeat - called by leader to check if worker is alive
func (s *WorkerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatReq) (*pb.HeartbeatRes, error) {
	return &pb.HeartbeatRes{IsAlive: true}, nil
}

// ListPrimes - compute primes (main task)
func (s *WorkerServer) ListPrimes(ctx context.Context, req *pb.PrimeReq) (*pb.PrimeRes, error) {
	primes := []int32{}
	for i := int32(2); i <= req.Num; i++ {
		isPrime := true
		for j := int32(2); j*j <= i; j++ {
			if i%j == 0 {
				isPrime = false
				break
			}
		}
		if isPrime {
			primes = append(primes, i)
		}
	}
	return &pb.PrimeRes{PrimeList: primes, ProcessedBy: s.workerID}, nil
}

// StartElection - Bully algorithm: if sender ID is lower, I have higher priority
func (s *WorkerServer) StartElection(ctx context.Context, req *pb.ElectionReq) (*pb.ElectionRes, error) {
	s.mu.RLock()
	myID := s.nodeID
	s.mu.RUnlock()

	log.Printf("[Election] Received election from node %d", req.SenderID)

	if req.SenderID < myID {
		log.Printf("[Election] My ID (%d) > %d, I take over", myID, req.SenderID)
		go s.runElection()
		return &pb.ElectionRes{Ok: true}, nil
	}

	return &pb.ElectionRes{Ok: false}, nil
}

// AnnounceLeader - accept new leader announcement
func (s *WorkerServer) AnnounceLeader(ctx context.Context, req *pb.LeaderReq) (*pb.LeaderRes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Election] Node %d is the new leader", req.LeaderID)
	s.currentLeaderID = req.LeaderID
	s.isLeader = (req.LeaderID == s.nodeID)
	s.inElection = false

	if s.isLeader {
		log.Printf("[Election] *** I AM THE NEW LEADER ***")
	}

	return &pb.LeaderRes{Ack: true}, nil
}

// runElection - Bully algorithm
func (s *WorkerServer) runElection() {
	s.mu.Lock()
	if s.inElection {
		s.mu.Unlock()
		return
	}
	s.inElection = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.inElection = false
		s.mu.Unlock()
	}()

	log.Printf("[Election] Starting election (my ID: %d)", s.nodeID)

	// Find all nodes with higher ID
	higherNodes := []int32{}
	s.mu.RLock()
	for peerID := range s.peers {
		if peerID > s.nodeID {
			higherNodes = append(higherNodes, peerID)
		}
	}
	s.mu.RUnlock()

	// If no higher nodes, I win
	if len(higherNodes) == 0 {
		log.Printf("[Election] No higher nodes - I WIN!")
		s.becomeLeader()
		return
	}

	// Send election to all higher nodes
	anyResponded := false
	for _, higherID := range higherNodes {
		s.mu.RLock()
		peerAddr := s.peers[higherID]
		s.mu.RUnlock()

		if peerAddr == "" {
			continue
		}

		conn, err := grpc.Dial(peerAddr, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
		if err != nil {
			continue
		}

		client := pb.NewElectionClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.StartElection(ctx, &pb.ElectionReq{SenderID: s.nodeID})
		cancel()
		conn.Close()

		if err == nil && resp.Ok {
			anyResponded = true
			log.Printf("[Election] Node %d responded", higherID)
			break
		}
	}

	// If no one responded, I win
	if !anyResponded {
		log.Printf("[Election] No responses - I WIN!")
		s.becomeLeader()
	} else {
		log.Printf("[Election] Higher node responded, waiting for announcement...")
	}
}

// becomeLeader - announce myself as leader to all peers
func (s *WorkerServer) becomeLeader() {
	s.mu.Lock()
	s.isLeader = true
	s.currentLeaderID = s.nodeID
	s.mu.Unlock()

	log.Printf("[Election] I am the leader!")

	// Announce to all peers
	s.mu.RLock()
	peers := s.peers
	s.mu.RUnlock()

	for peerID, peerAddr := range peers {
		go func(id int32, addr string) {
			conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
			if err != nil {
				return
			}
			defer conn.Close()

			client := pb.NewElectionClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			client.AnnounceLeader(ctx, &pb.LeaderReq{LeaderID: s.nodeID})
			cancel()
			log.Printf("[Election] Announced leadership to node %d", id)
		}(peerID, peerAddr)
	}
}

// monitorLeader - detect leader failure and trigger election
func (s *WorkerServer) monitorLeader() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	consecutiveFailures := 0

	for range ticker.C {
		s.mu.RLock()
		isLeader := s.isLeader
		currentLeaderID := s.currentLeaderID
		inElection := s.inElection
		s.mu.RUnlock()

		// Skip if I'm the leader or election in progress
		if isLeader || inElection {
			consecutiveFailures = 0
			continue
		}

		// Get leader address
		s.mu.RLock()
		leaderAddr := s.peers[currentLeaderID]
		s.mu.RUnlock()

		if leaderAddr == "" {
			log.Printf("[Monitor] Leader address unknown")
			consecutiveFailures++
			if consecutiveFailures >= 2 {
				log.Printf("[Monitor] No leader info - starting election")
				consecutiveFailures = 0
				go s.runElection()
			}
			continue
		}

		// Ping leader
		conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
		if err != nil {
			consecutiveFailures++
			if consecutiveFailures >= 2 {
				log.Printf("[Monitor] Leader %d is DEAD - starting election", currentLeaderID)
				consecutiveFailures = 0
				go s.runElection()
			}
			continue
		}

		client := pb.NewWorkerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.Heartbeat(ctx, &pb.HeartbeatReq{})
		cancel()
		conn.Close()

		if err != nil {
			consecutiveFailures++
			if consecutiveFailures >= 2 {
				log.Printf("[Monitor] Leader %d heartbeat failed - starting election", currentLeaderID)
				consecutiveFailures = 0
				go s.runElection()
			}
		} else {
			consecutiveFailures = 0
			log.Printf("[Monitor] Leader %d is alive", currentLeaderID)
		}
	}
}

func main() {
	workerID := os.Getenv("WORKER_ID")
	port := os.Getenv("PORT")
	nodeIDStr := os.Getenv("NODE_ID")
	peersStr := os.Getenv("PEERS")

	nodeID, _ := strconv.Atoi(nodeIDStr)

	// Parse peers: "1=worker1:50052,2=worker2:50053,3=leader:50051"
	peers := make(map[int32]string)
	if peersStr != "" {
		for _, peer := range strings.Split(peersStr, ",") {
			parts := strings.Split(peer, "=")
			if len(parts) == 2 {
				id, _ := strconv.Atoi(parts[0])
				peers[int32(id)] = parts[1]
			}
		}
	}

	worker := &WorkerServer{
		workerID:        workerID,
		nodeID:          int32(nodeID),
		port:            port,
		peers:           peers,
		currentLeaderID: 3, // Default: assume node 3 is leader
		isLeader:        false,
		inElection:      false,
	}

	// Start leader monitoring
	go worker.monitorLeader()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, worker)
	pb.RegisterSchedulerServer(s, worker)
	pb.RegisterElectionServer(s, worker)

	log.Printf("[Worker] %s listening on port %s (node ID: %d)", workerID, port, nodeID)

	// Register with leader
	go func() {
		time.Sleep(1 * time.Second)
		leaderAddr := worker.peers[3] // Node 3 is leader
		if leaderAddr == "" {
			return
		}
		conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure(), grpc.WithTimeout(3*time.Second))
		if err != nil {
			log.Printf("[Register] Failed to connect to leader: %v", err)
			return
		}
		defer conn.Close()
		client := pb.NewLeaderRegistryClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err = client.RegisterWorker(ctx, &pb.RegisterReq{
			WorkerID:   workerID,
			WorkerAddr: workerID + ":" + port,
		})
		cancel()
		if err != nil {
			log.Printf("[Register] Failed: %v", err)
		} else {
			log.Printf("[Register] Successfully registered with leader")
		}
	}()

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
