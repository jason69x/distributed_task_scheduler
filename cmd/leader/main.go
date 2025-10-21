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

type WorkerInfo struct {
	Address string
	IsAlive bool
}

type LeaderServer struct {
	pb.UnimplementedSchedulerServer
	pb.UnimplementedWorkerServer
	pb.UnimplementedElectionServer
	pb.UnimplementedLeaderRegistryServer

	workers map[string]*WorkerInfo
	workerMu sync.RWMutex

	nodeID          int32
	peers           map[int32]string
	currentLeaderID int32
	isLeader        bool
	inElection      bool
	mu              sync.RWMutex
}

// Heartbeat - workers call this to check if leader is alive
func (s *LeaderServer) Heartbeat(ctx context.Context, req *pb.HeartbeatReq) (*pb.HeartbeatRes, error) {
	return &pb.HeartbeatRes{IsAlive: true}, nil
}

// ListPrimes - delegate task to a worker
func (s *LeaderServer) ListPrimes(ctx context.Context, req *pb.PrimeReq) (*pb.PrimeRes, error) {
	s.workerMu.RLock()
	aliveWorkers := []string{}
	for _, info := range s.workers {
		if info.IsAlive {
			aliveWorkers = append(aliveWorkers, info.Address)
		}
	}
	s.workerMu.RUnlock()

	if len(aliveWorkers) == 0 {
		return nil, fmt.Errorf("no alive workers available")
	}

	// Pick first alive worker (simple strategy)
	workerAddr := aliveWorkers[0]
	log.Printf("[Task] Assigning to worker: %s", workerAddr)

	conn, err := grpc.Dial(workerAddr, grpc.WithInsecure(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewWorkerClient(conn)
	return client.ListPrimes(ctx, req)
}

// RegisterWorker - worker registers with leader
func (s *LeaderServer) RegisterWorker(ctx context.Context, req *pb.RegisterReq) (*pb.RegisterRes, error) {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()

	if existing, exists := s.workers[req.WorkerID]; exists {
		existing.IsAlive = true
		log.Printf("[Register] Worker %s re-registered", req.WorkerID)
	} else {
		s.workers[req.WorkerID] = &WorkerInfo{
			Address: req.WorkerAddr,
			IsAlive: true,
		}
		log.Printf("[Register] New worker: %s at %s", req.WorkerID, req.WorkerAddr)
	}

	return &pb.RegisterRes{
		Success: true,
		Msg:     fmt.Sprintf("worker %s registered", req.WorkerID),
	}, nil
}

// StartElection - Bully algorithm
func (s *LeaderServer) StartElection(ctx context.Context, req *pb.ElectionReq) (*pb.ElectionRes, error) {
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

// AnnounceLeader - accept new leader
func (s *LeaderServer) AnnounceLeader(ctx context.Context, req *pb.LeaderReq) (*pb.LeaderRes, error) {
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
func (s *LeaderServer) runElection() {
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
func (s *LeaderServer) becomeLeader() {
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

// monitorWorkers - health check workers
func (s *LeaderServer) monitorWorkers() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.workerMu.Lock()
		for workerID, info := range s.workers {
			conn, err := grpc.Dial(info.Address, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
			if err != nil {
				info.IsAlive = false
				log.Printf("[Monitor] Worker %s DEAD", workerID)
				continue
			}

			client := pb.NewWorkerClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = client.Heartbeat(ctx, &pb.HeartbeatReq{})
			cancel()
			conn.Close()

			if err != nil {
				info.IsAlive = false
				log.Printf("[Monitor] Worker %s heartbeat FAILED", workerID)
			} else {
				info.IsAlive = true
				log.Printf("[Monitor] Worker %s alive", workerID)
			}
		}

		alive := 0
		for _, info := range s.workers {
			if info.IsAlive {
				alive++
			}
		}
		log.Printf("[Monitor] Workers: %d alive, %d total", alive, len(s.workers))
		s.workerMu.Unlock()
	}
}

func main() {
	nodeIDStr := os.Getenv("NODE_ID")
	peersStr := os.Getenv("PEERS")

	nodeID, _ := strconv.Atoi(nodeIDStr)

	// Parse peers: "1=worker1:50052,2=worker2:50053"
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

	leader := &LeaderServer{
		workers:         make(map[string]*WorkerInfo),
		nodeID:          int32(nodeID),
		peers:           peers,
		currentLeaderID: int32(nodeID),
		isLeader:        true,
		inElection:      false,
	}

	// Start worker health monitoring
	go leader.monitorWorkers()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterSchedulerServer(s, leader)
	pb.RegisterElectionServer(s, leader)
	pb.RegisterLeaderRegistryServer(s, leader)

	log.Printf("[Leader] Node %d listening on port 50051", nodeID)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
