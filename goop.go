package goop

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// PubSub - Wrapper for Pub/Sub.
type PubSub struct {
	Context context.Context
	Project string
	Opts    []option.ClientOption
	Client  *pubsub.Client
}

// CreateClient - Create a new Pub/Sub client.
//
// Return:
//   error - An error if it occurred.
func (p *PubSub) CreateClient() error {
	pubSubClient, pubSubErr := pubsub.NewClient(p.Context, p.Project, p.Opts...)
	if pubSubErr != nil {
		return pubSubErr
	}

	// Set the client.
	p.Client = pubSubClient
	return nil
}

// CreateTopic - Create a new Pub/Sub topic if it does not already exist.
//
// Params:
//     topicName string - The name of the topic to create.
//
// Return:
//     *pubsub.Topic - Pointer to a Pub/Sub topic.
//     error - An error if it occurred.
func (p *PubSub) CreateTopic(topicName string) (*pubsub.Topic, error) {
	fmt.Println("Creating Pub/Sub topic.")

	// Check if the topic already exists.
	topic := p.Client.Topic(topicName)
	ok, err := topic.Exists(p.Context)
	if err != nil {
		return nil, err
	}

	// Bail out here if the topic already exists.
	if ok {
		fmt.Printf("Pub/Sub topic (%s) already exists.\n", topicName)
		return topic, nil
	}

	// Create a topic to subscribe to.
	topic, err = p.Client.CreateTopic(p.Context, topicName)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Created Pub/Sub topic (%s).\n", topicName)

	return topic, nil
}

// CreateSubscription - Create a new Pub/Sub subscription if it does not already
// exist.
//
// Params:
//     topic *pubsub.Topic - The topic to get messages from.
//     subName string - The name of the subscription to use.
//
// Return:
//     *pubsub.Topic - Pointer to a Pub/Sub subsription.
//     error - An error if it occurred.
func (p *PubSub) CreateSubscription(topic *pubsub.Topic, subName string) (*pubsub.Subscription, error) {
	fmt.Println("Creating Pub/Sub subscription.")

	// Check if the subscription already exists.
	sub := p.Client.Subscription(subName)
	ok, err := sub.Exists(p.Context)
	if err != nil {
		return nil, err
	}

	// Bail out here if the subscription already exists.
	if ok {
		fmt.Printf("Pub/Sub subscription (%s) already exists.\n", subName)
		return sub, nil
	}

	// Create a subscription.
	sub, err = p.Client.CreateSubscription(p.Context, subName, topic, 20*time.Second, nil)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Created Pub/Sub subscription (%s).\n", subName)
	return sub, nil
}

// PullMessages - Pull messages from a Pub/Sub subscription.
//
// Params:
//     subName string - The name of the subscription to use.
//     messageCallback func(msg *pubsub.Message, p *PubSub) error - Callback
//     function to fire for each message pulled from the topic.
//
// Return:
//     error - An error if it occurred.
func (p *PubSub) PullMessages(subName string, messageCallback func(msg *pubsub.Message, p *PubSub) error) error {
	// Get the subscription.
	sub := p.Client.Subscription(subName)
	it, err := sub.Pull(p.Context)
	if err != nil {
		return err
	}
	defer it.Stop()

	// Iterate over the messages.
	for {
		msg, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		// Run the callback function and ensure no errors occurred.
		if err := messageCallback(msg, p); err != nil {
			fmt.Fprintf(os.Stderr, "\nAn error occurred while pulling message (%s). Reason - %+v\n", msg.ID, err)
			continue
		}

		msg.Done(true)
	}

	return nil
}

// Publish - Publish a message to a Pub/Sub topic.
//
// Params:
//     topicName string - The name of the topic to publish to.
//     msg string - The message body to publish.
//
// Return:
//     error - An error if it occurred.
func (p *PubSub) Publish(topicName, msg string) error {
	// Get the topic.
	t := p.Client.Topic(topicName)

	// Publish the message.
	result := t.Publish(p.Context, &pubsub.Message{
		Data: []byte(msg),
	})

	// Check the result for any errors.
	_, err := result.Get(p.Context)
	if err != nil {
		return err
	}

	return nil
}

// PublishWithAttributes - Publish a message with attributes to a Pub/Sub topic.
//
// Params:
//     topicName string - The name of the topic to publish to.
//     msg string - The message body to publish.
//     attributes map[string]string - Map of attributes (key/value) to publish
//     along with the message body.
//
// Return:
//     error - An error if it occurred.
func (p *PubSub) PublishWithAttributes(topicName, msg string, attributes map[string]string) error {
	// Get the topic.
	t := p.Client.Topic(topicName)

	// Publish the message.
	result := t.Publish(p.Context, &pubsub.Message{
		Data:       []byte(msg),
		Attributes: attributes,
	})

	// Check the result for any errors.
	_, err := result.Get(p.Context)
	if err != nil {
		return err
	}

	return nil
}
