package commands

import (
	"context"
	"fmt"

	"github.com/kameshsampath/rp-registry-demo/pkg/addressbook"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type ConsumerOptions struct {
	topic   string
	seeds   []string
	groupID string
}

type Result struct {
	Record *kgo.Record
	Errors []kgo.FetchError
}

// AddFlags implements Command.
func (c *ConsumerOptions) AddFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&c.topic, "topic", "t", "addressbook", "The address book topic.")
	cmd.Flags().StringVarP(&c.groupID, "group-id", "g", "my-addressbook-group", "The address book consumer Group ID.")
	cmd.Flags().StringArrayVarP(&c.seeds, "seeds", "s", []string{"localhost:19092"}, "The Kafka brokers to be used to send the message.")
}

// Execute implements Command.
func (c *ConsumerOptions) Execute(cmd *cobra.Command, args []string) error {

	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.seeds...),
		kgo.ConsumerGroup(c.groupID),
		kgo.ConsumeTopics(c.topic),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ch := make(chan Result)
	go func() {
		pollAndPrint(client, ch)
	}()

	for {
		select {
		case r := <-ch:
			{
				if errs := r.Errors; len(errs) > 0 {
					log.Fatal(errs)
				}
				b := r.Record.Value
				var book addressbook.Person
				if err := proto.Unmarshal(b, &book); err != nil {
					return err
				} else {
					log.Infow("Record",
						"ID", book.Id,
						"Name", book.Name,
						"Email", book.Email,
					)
				}
			}
		}
	}
}

// Validate implements Command.
func (c *ConsumerOptions) Validate(cmd *cobra.Command, args []string) error {
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}
	return nil
}

func pollAndPrint(client *kgo.Client, ch chan Result) {
	log.Debugln("Started to poll topic until error")
	bctx := context.Background()

	//Consumer
	for {
		fetches := client.PollFetches(bctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			ch <- Result{
				Errors: errs,
			}
		}

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, r := range p.Records {
				ch <- Result{
					Record: r,
				}
			}
		})
	}
}

// ProducerOptions implements Interface
var _ Command = (*ConsumerOptions)(nil)
var consumerExample = fmt.Sprintf(`
  # Consume message default options
  %[1]s consume
  # Consume messages from specific topic
  %[1]s consume --topic phonebook
  # Consume messages from specific topicwith custom list of brokers
  %[1]s consume --topic phonebook --broker http://localhost:19092
`, ExamplePrefix())

// NewConsumerCommand instantiates the new instance of the ConsumerCommand
func NewConsumerCommand() *cobra.Command {
	consumerOpts := &ConsumerOptions{}

	consumerCmd := &cobra.Command{
		Use:     "consume",
		Short:   "Consume address book messages from a topic",
		Example: consumerExample,
		RunE:    consumerOpts.Execute,
		PreRunE: consumerOpts.Validate,
	}

	consumerOpts.AddFlags(consumerCmd)

	return consumerCmd
}
