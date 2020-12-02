package main

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	"google.golang.org/grpc/keepalive"

	data "github.com/jamoreno22/lab2_dist/datanode_3/pkg/proto"
	"google.golang.org/grpc"
)

type dataNodeServer struct {
	data.UnimplementedDataNodeServer
}

// books variable when books are saved
var books = []data.Book{}
var distributionType string
var bookName string
var bookParts int32

func main() {

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
	// implementar sendproposal nuevo sin esperar retorno desde el server
	if distributionType == "1" {
		stream, err := nc.SendProposalDist(context.Background())
		if err != nil {
			log.Fatalf("%v.RecordRoute(_) = _, %v", nc, err)
		}
		_, errD := stream.CloseAndRecv()
		if errD != nil {
			log.Fatalf("%v", errD)
		}
		runDistributeChunks(proposals)
	} else {
		stream, err := nc.SendProposal(context.Background())
		if err != nil {
			log.Println("Error de stream send proposal")
		}

		for _, prop := range proposals {

			if err := stream.Send(&prop); err != nil {
				log.Println("error al enviar chunk")
			}
		}
		stream.CloseSend()
		time.Sleep(5 * time.Second)
		finalProposals := []data.Proposal{}
		for {

			in, err := stream.Recv()
			if err == io.EOF || len(proposals) == len(finalProposals) {
				// read done.
				if distributionType == "0" {
					runDistributeChunks(finalProposals)
				}

				return nil
			}
			if err != nil {
				log.Fatalf("Failed to receive a proposal : %v", err)
			}
			//in es cada proposal
			finalProposals = append(finalProposals, *in)
		}
	}
	return nil
}

// - - - - - - - - - - - - - DataNode Server functions - - - - - - - - - - - -

//Distribute chunk server side

func (d *dataNodeServer) DistributeChunks(ctx context.Context, req *data.Chunk) (*data.Message, error) {
	os.Open("Chunks/")
	ioutil.WriteFile(req.Name, req.Data, os.ModeAppend)
	return &data.Message{Text: "Recibido"}, nil
}

//SendBookInfo
func (d *dataNodeServer) SendBookInfo(ctx context.Context, req *data.Book) (*data.Message, error) {
	bookName = req.Name
	bookParts = req.Parts
	return &data.Message{Text: "UwU"}, nil
}

//DistributionType server side
func (d *dataNodeServer) DistributionType(ctx context.Context, req *data.Message) (*data.Message, error) {
	distributionType = req.Text
	return &data.Message{Text: "Recibido"}, nil
}

// DistributeChunks in another datanodes
func runDistributeChunks(props []data.Proposal) error {
	//-----  crear las conexiones a los otros datanodes ----------------------
	var datanode1Conn *grpc.ClientConn
	datanode1Conn, err2 := grpc.Dial("10.10.28.17:9000", grpc.WithInsecure())
	if err2 != nil {
		log.Fatalf("did not connect: %s", err2)
	}

	// Datanode_2 Connection -------------------------------------------
	var datanode2Conn *grpc.ClientConn
	datanode2Conn, err3 := grpc.Dial("10.10.28.18:9000", grpc.WithInsecure())
	if err3 != nil {
		log.Fatalf("did not connect: %s", err3)
	}

	for _, prop := range props {
		if prop.Ip == "10.10.28.19:9000" {
			// write/save buffer to disk
			os.Open("Chunks/")
			ioutil.WriteFile(prop.Chunk.Name, prop.Chunk.Data, os.ModeAppend)
		} else if prop.Ip == "10.10.28.17:9000" {
			datanode1Client := data.NewDataNodeClient(datanode1Conn)
			_, err := datanode1Client.DistributeChunks(context.Background(), prop.Chunk)
			if err != nil {
				log.Printf("%v", err)
			}
		} else if prop.Ip == "10.10.28.18:9000" {
			datanode2Client := data.NewDataNodeClient(datanode2Conn)
			_, err := datanode2Client.DistributeChunks(context.Background(), prop.Chunk)
			if err != nil {
				log.Printf("%v", err)
			}
		}
	}
	defer datanode1Conn.Close()
	defer datanode2Conn.Close()
	return nil
}

// pingproposal
func (d *dataNodeServer) PingProposal(srv data.DataNode_PingProposalServer) error {
	prop := []data.Proposal{}
	for {
		in, err := srv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		prop = append(prop, *in)
	}
	for _, pro := range prop {
		if err := srv.Send(&pro); err != nil {
			return err
		}
	}
	return nil
}

// UploadBook server side
func (d *dataNodeServer) UploadBook(ubs data.DataNode_UploadBookServer) error {
	book := data.Book{}
	indice := 0
	for {
		chunk, err := ubs.Recv()
		if err == io.EOF {
			books = append(books, book)

			prop := generateProposals(book, []string{"10.10.28.17:9000", "10.10.28.18:9000", "10.10.28.19:9000"})

			if distributionType == "1" {

				//chequear propuesta entre datanodes
				flag1 := false
				flag2 := false

				//-----  crear las conexiones a los otros datanodes ----------------------
				var datanode1Conn *grpc.ClientConn
				datanode1Conn, err2 := grpc.Dial("10.10.28.17:9000", grpc.WithInsecure())
				if err2 != nil {
					flag1 = false
				}

				// Datanode_2 Connection -------------------------------------------
				var datanode2Conn *grpc.ClientConn
				datanode2Conn, err3 := grpc.Dial("10.10.28.18:9000", grpc.WithInsecure())
				if err3 != nil {
					flag2 = false
				}

				// envio de propuestas al datanode 1
				datanode1Client := data.NewDataNodeClient(datanode1Conn)
				streamdata1, _ := datanode1Client.PingProposal(context.Background())

				for _, pro := range prop {
					if err := streamdata1.Send(&pro); err != nil {
						log.Fatalf("Failed to send a proposal: %v", err)
					}
				}
				streamdata1.CloseSend()

				for {
					_, err := streamdata1.Recv()
					if err == io.EOF {
						flag1 = true
						break
					}
					if err != nil {
						log.Fatalf("Failed to receive a note : %v", err)
					}
				}

				// envio de propuesta al datanode 2
				datanode2Client := data.NewDataNodeClient(datanode2Conn)
				streamdata2, _ := datanode2Client.PingProposal(context.Background())

				for _, pro := range prop {
					if err := streamdata2.Send(&pro); err != nil {
						log.Fatalf("Failed to send a proposal: %v", err)
					}
				}
				streamdata2.CloseSend()

				for {
					_, err := streamdata2.Recv()
					if err == io.EOF {
						flag2 = true
						break
					}
					if err != nil {
						log.Fatalf("Failed to receive a note : %v", err)
					}
				}

				if !(flag1 && flag2 == true) {
					b, i := checkProposal(prop)
					if !b {
						prop = generateProposals(book, i)
					}
				}
			}

			// NameNodeServer Connection ---------------------------------------
			var nameConn *grpc.ClientConn
			nameConn, err := grpc.Dial("10.10.28.20:9000", grpc.WithInsecure(), grpc.WithKeepaliveParams(keepalive.ClientParameters{}))
			if err != nil {
				log.Fatalf("Did not connect: %s", err)
			}

			nameClient := data.NewNameNodeClient(nameConn)
			//-------------------------------------------------------------------

			// Send Book Info to NameNodeServer
			_, errd := nameClient.GetDistribution(context.Background(), &data.Message{Text: distributionType})
			if errd != nil {
				log.Printf("%v", errd)
			}
			_, err4 := nameClient.GetBookInfo(context.Background(), &data.Book{Name: bookName, Parts: bookParts})
			if err4 != nil {
				log.Fatalf("Did not connect: %s", err4)
			}
			runSendProposal(nameClient, prop)
			defer nameConn.Close()
			return (ubs.SendAndClose(&data.Message{Text: "EOF"}))
		}
		if err != nil {
			return err
		}
		book.Chunks = append(book.Chunks, chunk)
		indice = indice + 1

	}
}

// Download Book
func (d *dataNodeServer) DownloadBook(ctx context.Context, req *data.Chunk) (*data.Chunk, error) {
	os.Open("Chunks/")
	chunk := data.Chunk{}
	chunk.Data, _ = ioutil.ReadFile(req.Name)

	return &chunk, nil
}

// Rescatar Chunks
func (d *dataNodeServer) RescueChunks(ctx context.Context, req *data.Message) (*data.Proposal, error) {
	prop := data.Proposal{}
	prop.Chunk.Data, _ = ioutil.ReadFile(prop.Chunk.Name)
	return &prop, nil
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
