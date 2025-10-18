package main

import(
	"fmt"
	"google.golang.org/grpc"
	primeser "dist_scheduler/proto"
	"context"
	"os"
)

func main(){
	conn,_ := grpc.Dial("localhost:50051",grpc.WithInsecure())
	client := primeser.NewPrimeServiceClient(conn)

	resp,err := client.ListPrimes(context.Background(),&primeser.InputNum{Num: 53})
	if err!=nil{
		fmt.Println("server not running or some other error")
		os.Exit(0);
	}
	for _,val := range resp.PrimesList{
		fmt.Printf("%v ",val)
	}
}
