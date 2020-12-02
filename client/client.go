package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"

	data "github.com/jamoreno22/lab2_dist/datanode_3/pkg/proto"
	"google.golang.org/grpc"
)

var bookName string

func main() {
	var conn *grpc.ClientConn

	conn, err := grpc.Dial("10.10.28.19:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}

	defer conn.Close()

	dc := data.NewDataNodeClient(conn)

	fmt.Println("Seleccione qué desea hacer:")
	fmt.Println("0 : Cargar un libro")
	fmt.Println("1 : Descargar un libro")

	reader := bufio.NewReader(os.Stdin)
	char, _, err := reader.ReadRune()

	if err != nil {
		fmt.Println(err)
	}

	switch char {
	//Upload
	case '0':
		fmt.Println("Carga")
		fmt.Println("Seleccione distribución:")
		fmt.Println("0 : Centralizada")
		fmt.Println("1 : Distribuida")
		r := bufio.NewReader(os.Stdin)
		c, _, err := r.ReadRune()

		if err != nil {
			fmt.Println(err)
		}
		switch c {
		//Centralizado
		case '0':
			dc.DistributionType(context.Background(), &data.Message{Text: "0"})
			fileToBeChunked := "books/Mujercitas-Alcott_Louisa_May.pdf"
			bookName = "Mujercitas-Alcott_Louisa_May.pdf"
			runUploadBook(dc, fileToBeChunked)
			break
		//Distribuido
		case '1':
			dc.DistributionType(context.Background(), &data.Message{Text: "1"})
			fileToBeChunked := "books/Mujercitas-Alcott_Louisa_May.pdf"
			bookName = "Mujercitas-Alcott_Louisa_May.pdf"
			runUploadBook(dc, fileToBeChunked)
			break
		}
		break
	//Download
	case '1':
		fmt.Println("Ingrese nombre del libro a descargar: ")
		//r := bufio.NewReader(os.Stdin)
		//c, _, err := r.ReadRune()

		if err != nil {
			fmt.Println(err)
		}
		runDownloadBook(dc, "books/Mujercitas-Alcott_Louisa_May.pdf")
		fmt.Println("Descargado")
		break
	}

}

func runUploadBook(dc data.DataNodeClient, fileToBeChunked string) error {
	// -    - - - - - - -  - -    particionar pdf en chunks - - - - -  - - - -

	file, err := os.Open(fileToBeChunked)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 250000 // 1 MB, change this to your requirement

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	book := make([]*data.Chunk, totalPartsNum)
	part := 1

	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		// write to disk
		fileName := "part_" + strconv.Itoa(part) + "_" + strconv.Itoa(int(totalPartsNum))

		// books instantiation
		book[i] = &data.Chunk{Name: fileName, Data: partBuffer}
		part = part + 1
	}
	// - -- -- - - -- -  Send book info
	dc.SendBookInfo(context.Background(), &data.Book{Name: bookName, Parts: int32(len(book))})
	// - - - - - --- -- - -  stream chunks - - - - - - - - - - - -
	stream, err := dc.UploadBook(context.Background())
	if err != nil {
		log.Println("Error de stream uploadBook")
	}
	a := 1
	for _, chunk := range book {
		if err := stream.Send(chunk); err != nil {
			log.Println("error al enviar chunk")
			log.Fatalf("%v.Send(%d) = %v", stream, a, err)
		}
		a = a + 1
	}
	_, errLast := stream.CloseAndRecv()
	if errLast != nil {
		log.Println("Error recepcion response")
		return errLast
	}
	log.Printf("El libro ha sido subido correctamente")
	return nil
}

func runDownloadBook(dc data.DataNodeClient, msg string) error {
	var chunks []data.Chunk
	stream, err := dc.DownloadBook(context.Background(), &data.Message{Text: msg})
	if err != nil {
		return err
	}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			rebuildBook(chunks)
			return nil
		}
		if err != nil {
			log.Fatalf("%v.ListFeatures(_) = _, %v", dc, err)
		}
		chunks = append(chunks, *chunk)
	}
}

func rebuildBook(chunks []data.Chunk) error {

	newFileName := bookName + ".zip"
	_, err := os.Create(newFileName)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//set the newFileName file to APPEND MODE!!
	// open files r and w

	file, err := os.OpenFile(newFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// IMPORTANT! do not defer a file.Close when opening a file for APPEND mode!
	// defer file.Close()

	// just information on which part of the new file we are appending
	var writePosition int64 = 0

	for j := uint64(0); j < uint64(len(chunks)); j++ {

		//read a chunk
		currentChunkFileName := bookName + strconv.FormatUint(j, 10)

		newFileChunk, err := os.Open(currentChunkFileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		defer newFileChunk.Close()

		chunkInfo, err := newFileChunk.Stat()

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// calculate the bytes size of each chunk
		// we are not going to rely on previous data and constant

		var chunkSize int64 = chunkInfo.Size()
		chunkBufferBytes := make([]byte, chunkSize)

		//fmt.Println("Appending at position : [", writePosition, "] bytes")
		writePosition = writePosition + chunkSize

		// read into chunkBufferBytes
		reader := bufio.NewReader(newFileChunk)
		_, err = reader.Read(chunkBufferBytes)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// DON't USE ioutil.WriteFile -- it will overwrite the previous bytes!
		// write/save buffer to disk
		//ioutil.WriteFile(newFileName, chunkBufferBytes, os.ModeAppend)

		n, err := file.Write(chunkBufferBytes)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		file.Sync() //flush to disk

		// free up the buffer for next cycle
		// should not be a problem if the chunk size is small, but
		// can be resource hogging if the chunk size is huge.
		// also a good practice to clean up your own plate after eating

		chunkBufferBytes = nil // reset or empty our buffer

		fmt.Println("Written ", n, " bytes")

		fmt.Println("Recombining part [", j, "] into : ", newFileName)
	}

	// now, we close the newFileName
	file.Close()

	return nil
}
