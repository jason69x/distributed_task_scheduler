package main

import (
	"context"
	primeser "dist_scheduler/proto"
	"fmt"
	"net"

	"google.golang.org/grpc"
)

type PrimeServer struct {
	primeser.UnimplementedPrimeServiceServer
}

func (s *PrimeServer) CheckPrime(ctx context.Context,req *primeser.InputNum)(*primeser.OutputResult,error){
	n := req.Num
	if n<2 {
		return &primeser.OutputResult{IsPrime: "not a prime"},nil
	}
	var i int32 
	for i=2;i*i<n;i++{
		if n%i==0 {
			return &primeser.OutputResult{IsPrime: "not a prime"},nil
		}
	}
	return &primeser.OutputResult{IsPrime:"its prime :)"}, nil
}

func (s *PrimeServer) ListPrimes(ctx context.Context,req *primeser.InputNum)(*primeser.OutputList,error){
	n := req.Num
	list := []int32{}
	var i int32
	for i=2;i<n;i++ {
		var j int32
		var res bool = true 
		for j=2;j*j<i;j++{
			if i%j == 0 {
				res = false
				break
			}
		}
		if res {
			list = append(list, i)
		}
	}	
	return &primeser.OutputList{PrimesList: list},nil
} 

func main(){
	lis,_ := net.Listen("tcp",":50051")
	grpcServer := grpc.NewServer()
	primeser.RegisterPrimeServiceServer(grpcServer,&PrimeServer{})
	fmt.Println("server listening on :50051")
	grpcServer.Serve(lis)
}
