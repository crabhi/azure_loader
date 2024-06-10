package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/palantir/stacktrace"
)

type AzureTier struct {
	val *blob.AccessTier
}

func (s *AzureTier) String() string {
	if s.val == nil {
		return ""
	}
	return string(*s.val)
}

func (s *AzureTier) Set(v string) error {
	if v == "" {
		v = "Hot"
	}
	for _, possibleAccessTier := range blob.PossibleAccessTierValues() {
		if string(possibleAccessTier) == v {
			s.val = &possibleAccessTier
			break
		}
	}
	if s.val == nil {
		return stacktrace.NewError("Azure tier %s not found", v)
	}
	return nil
}

func (s *AzureTier) Get() *blob.AccessTier {
	return s.val
}

var inputFileName = flag.String("input", "-", "Input file to read. Use - for stdin.")
var azureUrl = flag.String("azure-url", "", "https://<storage-account-name>.blob.core.windows.net/")
var azureTier = &AzureTier{}
var concurrency = flag.Int("j", 1, "Number of concurrent transfers.")

var errCloseSentinel = errors.New("finished")

func init() {
	flag.Var(azureTier, "azure-tier", "Azure tier: Hot, Cool, Cold, Archive, ...")
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type CopyWorkItem struct {
	Bucket string
	Key    string
}

type FromS3Copier struct {
	s3Client *s3.Client
	azClient *azblob.Client
	ctx      context.Context
}

func (c *FromS3Copier) CopyFile(wi *CopyWorkItem) error {
	key, err := url.QueryUnescape(wi.Key)
	if err != nil {
		return stacktrace.Propagate(err, "unable to url decode %s/%s", wi.Bucket, wi.Key)
	}
	out, err := c.s3Client.GetObject(c.ctx, &s3.GetObjectInput{
		Bucket: &wi.Bucket,
		Key:    &key,
	})
	if err != nil {
		return stacktrace.Propagate(err, "%s\t%s download failed", wi.Bucket, key)
	}
	defer out.Body.Close()

	azureContainer := strings.ReplaceAll(wi.Bucket, ".", "-")
	_, err = c.azClient.UploadStream(c.ctx, azureContainer, key, out.Body, &azblob.UploadStreamOptions{AccessTier: azureTier.Get()})
	if err != nil {
		if responseError, ok := stacktrace.RootCause(err).(*azcore.ResponseError); ok {
			return fmt.Errorf("%s\t%s upload failed: %s", azureContainer, key, responseError.ErrorCode)
		}
		return stacktrace.Propagate(err, "Uploading %s/%s failed", azureContainer, key)
	}

	log.Printf("Copied\t%s\t%s\n", wi.Bucket, key)
	return nil
}

func NewS3Copier() (*FromS3Copier, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, stacktrace.Propagate(err, "Couldn't prepare Azure credentials - try running `az login`")
	}

	azClient, err := azblob.NewClient(*azureUrl, credential, nil)
	if err != nil {
		return nil, stacktrace.Propagate(err, "Azure client")
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	s3Client := s3.NewFromConfig(cfg)

	c := FromS3Copier{
		azClient: azClient,
		s3Client: s3Client,
		ctx:      context.Background(),
	}
	return &c, nil
}

func (c *FromS3Copier) DoCopy(inputs chan *CopyWorkItem, resultsChannel chan error) {
	for workItem := range inputs {
		if c.ctx.Err() != nil {
			break
		}
		err := c.CopyFile(workItem)
		resultsChannel <- err
	}
	resultsChannel <- errCloseSentinel
}

func readerLoop() chan *CopyWorkItem {
	output := make(chan *CopyWorkItem)
	var inputFile io.Reader
	var inputFileCloser io.Closer = nil
	if *inputFileName == "-" {
		inputFile = os.Stdin
	} else {
		f, err := os.Open(*inputFileName)
		handleError(err)
		inputFile = f
		inputFileCloser = f
	}
	workReader := csv.NewReader(inputFile)
	workReader.Comma = '\t'

	go func() {
		defer close(output)
		if inputFileCloser != nil {
			defer inputFileCloser.Close()
		}
		for i := 1; true; i++ {
			line, err := workReader.Read()

			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Fatalf("Error reading input at line %d: %s", i, err)
				}
			}

			if len(line) != 2 {
				log.Fatalf("Wrong number of fields at line %d: %d fields", i, len(line))
			}

			output <- &CopyWorkItem{Bucket: line[0], Key: line[1]}
		}
	}()

	return output
}

func main() {
	flag.Parse()

	inputs := readerLoop()
	results := make(chan error, 2*(*concurrency))

	workers := make([]*FromS3Copier, 0, *concurrency)
	for i := len(workers); i < *concurrency; i++ {
		c, err := NewS3Copier()
		handleError(err)
		workers = append(workers, c)
		go c.DoCopy(inputs, results)
	}

	expectedCloses := *concurrency
	seenOk := 0
	seen := 0
	for res := range results {
		if res == errCloseSentinel {
			expectedCloses -= 1
			if expectedCloses == 0 {
				close(results)
				return
			}
		}

		seen += 1
		if res == nil {
			seenOk += 1
		} else if res == errCloseSentinel {
			// pass
		} else {
			log.Printf("Error: %s", res)
		}
	}
	log.Printf("Processed %d (%d errors)", seen, seen-seenOk)
}
