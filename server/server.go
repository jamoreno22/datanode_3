package main

import (
	"context"
	"io"
	"log"
	"math/rand"
	"net"
	"time"

	data "github.com/jamoreno22/lab2_dist/datanode_3/pkg/proto"
	"google.golang.org/grpc"
)

type dataNodeServer struct {
	data.UnimplementedDataNodeServer
}

// books variable when books are saved
var books = []data.Book{}
var distributionType string

func main() {
	// NameNodeServer Connection ---------------------------------------
	var nameConn *grpc.ClientConn

	nameConn, err := grpc.Dial("10.10.28.20:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Did not connect: %s", err)
	}

	defer nameConn.Close()

	nameClient := data.NewNameNodeClient(nameConn)
	proposals := []data.Proposal{}
	proposals = append(proposals, data.Proposal{Ip: "8000", Chunk: &data.Chunk{Name: "Chunk1", Data: []byte("ABC€")}})
	proposals = append(proposals, data.Proposal{Ip: "8001", Chunk: &data.Chunk{Name: "Chunk2", Data: []byte("ABC2")}})

	runSendProposal(nameClient, proposals)

	bookName := "Mujercitas-Alcott_Louisa_May.pdf"

	runGetChunkDistribution(nameClient, &data.Message{Text: bookName})

	resp, err := nameClient.GetBookInfo(context.Background(), &data.Book{Name: bookName, Parts: 3})
	if err != nil {
		log.Fatalf("Did not connect: %s", err)
	}
	log.Println(resp)

	// Datanode_1 Connection -------------------------------------------
	var datanode1Conn *grpc.ClientConn

	datanode1Conn, err2 := grpc.Dial("10.10.28.17:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err2)
	}

	defer datanode1Conn.Close()

	// Datanode_2 Connection -------------------------------------------
	var datanode2Conn *grpc.ClientConn

	datanode2Conn, err3 := grpc.Dial("10.10.28.18:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err3)
	}

	defer datanode2Conn.Close()

	// Server Logic ----------------------------------------------------
	// create a listener on TCP port 9000
	lis, err := net.Listen("tcp", "10.10.28.19:9000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// create a server instance
	ds := dataNodeServer{}                               // create a gRPC server object
	grpcDataNodeServer := grpc.NewServer()               // attach the Ping service to the server
	data.RegisterDataNodeServer(grpcDataNodeServer, &ds) // start the server

	log.Println("DataNode Server running ...")
	if err := grpcDataNodeServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

}

//- - - - - - - - - - -  - - NameNode Client functions - - - - - -  -- - - - -

func runGetChunkDistribution(nc data.NameNodeClient, bookName *data.Message) ([]data.Proposal, error) {
	stream, err := nc.GetChunkDistribution(context.Background(), bookName)
	if err != nil {
		log.Printf("%v", err)
	}
	proposals := []data.Proposal{}
	for {
		feature, err := stream.Recv()
		if err == io.EOF {
			return proposals, nil
		}
		if err != nil {
			log.Fatalf("%v.ListFeatures(_) = _, %v", nc, err)
		}
		proposals = append(proposals, *feature)
	}
}

func runSendProposal(nc data.NameNodeClient, proposals []data.Proposal) error {

	stream, err := nc.SendProposal(context.Background())
	if err != nil {
		log.Println("Error de stream send proposal")
	}

	log.Println("ki voy")
	a := 1
	for _, prop := range proposals {

		if err := stream.Send(&prop); err != nil {
			log.Println("error al enviar chunk")
			log.Fatalf("%v.Send(%d) = %v", stream, a, err)
		}
		a = a + 1
	}
	for {
		log.Println("ki voy")

		in, err := stream.Recv()
		if err == io.EOF {
			// read done.
			//DistributeChunks()
			log.Printf("weno")
			return nil
		}
		if err != nil {
			log.Fatalf("Failed to receive a proposal : %v", err)
		}
		log.Printf("Got a proposal ip :%s ", in.Ip)
	}
}

// - - - - - - - - - - - - - DataNode Server functions - - - - - - - - - - - -

//DistributionType server side
func (d *dataNodeServer) DistributionType(ctx context.Context, req *data.Message) (*data.Message, error) {
	distributionType = req.Text
	return &data.Message{Text: "Recibido"}, nil
}

// DistributeChunks server side
func (d *dataNodeServer) DistributeChunks(dcs data.DataNode_DistributeChunksServer) error {
	log.Printf("Stream DistributeChunks")

	sP := []data.Proposal{}

	for {
		prop, err := dcs.Recv()
		if err == io.EOF {
			return (dcs.SendAndClose(&data.Message{Text: "Oh no... EOF"}))
		}
		if err != nil {
			return err
		}

		sP = append(sP, *prop)
		return nil
	}
}

// UploadBook server side

func (d *dataNodeServer) UploadBook(ubs data.DataNode_UploadBookServer) error {
	log.Printf("Stream UploadBook")
	book := data.Book{}
	indice := 0
	for {
		chunk, err := ubs.Recv()
		if err == io.EOF {
			books = append(books, book)

			prop := generateProposals(book, []string{"10.10.28.17:9000", "10.10.28.18:9000", "10.10.28.19:9000"})

			if distributionType == "1" {

				b, i := checkProposal(prop)
				if !b {
					prop = generateProposals(book, i)
				}

			}

			//enviar prop al server del NameNode

			return (ubs.SendAndClose(&data.Message{Text: "EOF"}))
		}
		if err != nil {
			return err
		}
		book.Chunks = append(book.Chunks, chunk)
		indice = indice + 1

	}
}

func generateProposals(book data.Book, Ips []string) []data.Proposal {
	var props []data.Proposal
	for _, chunk := range book.Chunks {
		randomIP := Ips[rand.Intn(len(Ips))]
		props = append(props, data.Proposal{Ip: randomIP, Chunk: chunk})
	}
	return props
}

func checkProposal(props []data.Proposal) (bool, []string) {
	var ips = []string{"10.10.28.17:9000", "10.10.28.18:9000", "10.10.28.19:9000"}
	var gIps []string

	for _, ip := range ips {
		if pingDataNode(ip) {
			gIps = append(gIps, ip)
		}
	}

	for _, prop := range props {
		if !stringInSlice(prop.Ip, gIps) {
			return false, gIps
		}
	}
	return true, gIps
}

func pingDataNode(ip string) bool {
	timeOut := time.Duration(10 * time.Second)
	_, err := net.DialTimeout("tcp", ip, timeOut)
	if err != nil {
		return false
	}
	return true
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
