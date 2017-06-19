package goop

import (
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsub"

	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

// Goop - Wrapper for Google Cloud Pub/Sub.
type Goop struct {
	Context context.Context
	Project string
	Opts    []option.ClientOption
	Client  *pubsub.Client
}

// CreateClient - Create a new Pub/Sub client.
//
// Return:
//   error - An error if it occurred.
func (g *Goop) CreateClient() error {
	pubSubClient, pubSubErr := pubsub.NewClient(g.Context, g.Project, g.Opts...)
	if pubSubErr != nil {
		return pubSubErr
	}

	// Set the client.
	g.Client = pubSubClient
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
func (g *Goop) CreateTopic(topicName string) (*pubsub.Topic, error) {
	fmt.Println("Creating Pub/Sub topic.")

	// Check if the topic already exists.
	topic := g.Client.Topic(topicName)
	ok, err := topic.Exists(g.Context)
	if err != nil {
		return nil, err
	}

	// Bail out here if the topic already exists.
	if ok {
		fmt.Printf("Pub/Sub topic (%s) already exists.\n", topicName)
		return topic, nil
	}

	// Create a topic to subscribe to.
	topic, err = g.Client.CreateTopic(g.Context, topicName)
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
func (g *Goop) CreateSubscription(topic *pubsub.Topic, subName string) (*pubsub.Subscription, error) {
	fmt.Println("Creating Pub/Sub subscription.")

	// Check if the subscription already exists.
	sub := g.Client.Subscription(subName)
	ok, err := sub.Exists(g.Context)
	if err != nil {
		return nil, err
	}

	// Bail out here if the subscription already exists.
	if ok {
		fmt.Printf("Pub/Sub subscription (%s) already exists.\n", subName)
		return sub, nil
	}

	// Create a subscription.
	sub, err = g.Client.CreateSubscription(g.Context, subName, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 20 * time.Second,
	})
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
//     messageCallback func(msg *pubsub.Message, g *Goop) error - Callback
//     function to fire for each message pulled from the topic.
//
// Return:
//     error - An error if it occurred.
func (g *Goop) PullMessages(subName string, messageCallback func(msg *pubsub.Message, g *Goop) error) error {
	// Get the subscription.
	sub := g.Client.Subscription(subName)

	// Receive the message from Pub/Sub.
	err := sub.Receive(g.Context, func(ctx context.Context, msg *pubsub.Message) {
		// Run our callback function.
		if err := messageCallback(msg, g); err != nil {
			log.Printf("Could not process message: %#v", err)
			msg.Nack()
			return
		}

		// Acknowledge receipt of the message.
		msg.Ack()
	})
	if err != nil {
		return err
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
func (g *Goop) Publish(topicName, msg string) error {
	// Get the topic.
	t := g.Client.Topic(topicName)

	// Publish the message.
	result := t.Publish(g.Context, &pubsub.Message{
		Data: []byte(msg),
	})

	// Check the result for any errors.
	_, err := result.Get(g.Context)
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
func (g *Goop) PublishWithAttributes(topicName, msg string, attributes map[string]string) error {
	// Get the topic.
	t := g.Client.Topic(topicName)

	// Publish the message.
	result := t.Publish(g.Context, &pubsub.Message{
		Data:       []byte(msg),
		Attributes: attributes,
	})

	// Check the result for any errors.
	_, err := result.Get(g.Context)
	if err != nil {
		return err
	}

	return nil
}
