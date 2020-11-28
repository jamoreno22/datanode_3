package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	gral "github.com/jamoreno22/lab2_dist/pkg/proto"
	"google.golang.org/grpc"
)

type dataServer struct {
	gral.UnimplementedDataNodeServer
}

var path = "Log"

// books variable when books are saved
var books = []gral.Book{}

func main() {

	// create a listener on TCP port 7777
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 7777))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// create a server instance
	ds := dataServer{}                               // create a gRPC server object
	grpcDataServer := grpc.NewServer()               // attach the Ping service to the server
	gral.RegisterDataNodeServer(grpcDataServer, &ds) // start the server

	log.Println("Server running ...")
	if err := grpcDataServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

}

// - - - - - - - - - - - - - DataNode Server functions - - - - - - - - - - - - 

// DistributeChunks server side
func (d *dataServer) DistributeChunks(dcs gral.DataNode_DistributeChunksServer) error {
	log.Printf("Stream DistributeChunks")

	sP := []gral.Proposal{}

	for {
		prop, err := dcs.Recv()
		if err == io.EOF {
			log.Printf("EOF ------------")
			return (dcs.SendAndClose(&gral.Message{Text: "Oh no... EOF"}))
		}
		if err != nil {
			return err
		}

		sP = append(sP, *prop)

	return nil
}

// UploadBook server side
func (d *dataServer) UploadBook(ubs gral.DataNode_UploadBookServer) error {
	log.Printf("Stream UploadBook")

	book := gral.Book{}
	indice := 0
	for {
		chunk, err := ubs.Recv()
		if err == io.EOF {
			books = append(books, book)
			log.Printf("EOF... books lenght = %d", len(books))
			return (ubs.SendAndClose(&gral.Message{Text: "EOF"}))
		}
		if err != nil {
			return err
		}
		book.Chunks = append(book.Chunks, chunk)
		indice = indice + 1

	}
}