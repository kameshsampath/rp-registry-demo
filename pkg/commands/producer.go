package commands

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/kameshsampath/rp-registry-demo/pkg/addressbook"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type ProducerOptions struct {
	topic    string
	seeds    []string
	dataFile string
}

// AddFlags implements Command.
func (p *ProducerOptions) AddFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&p.topic, "topic", "t", "addressbook", "The addressbook topic.")
	cmd.Flags().StringVarP(&p.dataFile, "data-file", "f", "", "The address book record data JSON file.")
	if err := cmd.MarkFlagRequired("data-file"); err != nil {
		log.Fatalf("Error marking flag 'data-file' as required %v", err)
	}
	cmd.Flags().StringArrayVarP(&p.seeds, "seeds", "s", []string{"localhost:19092"}, "The Kafka brokers to be used to send the message.")
}

// Execute implements Command.
func (p *ProducerOptions) Execute(cmd *cobra.Command, args []string) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(p.seeds...),
	)

	if err != nil {
		return err
	}
	defer client.Close()

	//Produce
	bctx := context.Background()
	tctx, cancel := context.WithTimeout(bctx, 3*time.Second)
	defer cancel()

	//Async Producer
	var wg sync.WaitGroup
	wg.Add(1)
	in, err := os.ReadFile(p.dataFile)
	if err != nil {
		log.Fatal(err)
	}
	log.Debugf("Data to be sent:\n", string(in))

	var book addressbook.Person
	if err := protojson.Unmarshal(in, &book); err != nil {
		return err
	}

	out, err := proto.Marshal(&book)
	if err != nil {
		return err
	}

	r := &kgo.Record{
		Key:   []byte(fmt.Sprintf("%d", book.Id)),
		Value: out,
		Topic: p.topic,
	}

	client.Produce(tctx, r, func(r *kgo.Record, err error) {
		if err != nil {
			log.Fatal(err)
		}
		defer wg.Done()
		log.Infow("Saved record",
			"Partition", r.Partition, "Offset", r.Offset)
	})
	wg.Wait()

	return nil
}

// Validate implements Command.
func (p *ProducerOptions) Validate(cmd *cobra.Command, args []string) error {
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}
	return nil
}

// ProducerOptions implements Interface
var _ Command = (*ProducerOptions)(nil)
var produceExample = fmt.Sprintf(`
  # Produce message default options
  %[1]s produce --data-file greetings.json
  # Produce message to Topic
  %[1]s produce --data-file greetings.json --topic phonebook
  # Produce message to Topic with custom list of brokers
  %[1]s produce --data-file greetings.json --topic greetings --broker http://localhost:19092
`, ExamplePrefix())

// NewProducerCommand instantiates the new instance of the StartCommand
func NewProducerCommand() *cobra.Command {
	producerOpts := &ProducerOptions{}

	producerCmd := &cobra.Command{
		Use:     "produce",
		Short:   "Send a address book message to topic",
		Example: produceExample,
		RunE:    producerOpts.Execute,
		PreRunE: producerOpts.Validate,
	}

	producerOpts.AddFlags(producerCmd)

	return producerCmd
}
