package goop

import (
	"fmt"
	"log"

	"golang.org/x/net/context"

	"cloud.google.com/go/pubsub"
)

// ExampleGoop_CreateClient - Example usage of the CreateClient function.
func ExampleGoop_CreateClient() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	g := &Goop{
		Context: ctx,
		Project: "project-id",
	}

	// Create the client.
	if err := g.CreateClient(); err != nil {
		log.Fatalf("\nFailed to create Pub/Sub client. Reason - %+v\n", err)
	}
}

// ExampleGoop_CreateTopic - Example usage of the CreateTopic function.
func ExampleGoop_CreateTopic() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	g := &Goop{
		Context: ctx,
		Project: "project-id",
	}

	// Create the client.
	if err := g.CreateClient(); err != nil {
		log.Fatalf("\nFailed to create Pub/Sub client. Reason - %+v\n", err)
	}

	// Create the topic.
	topic, err := g.CreateTopic("my-awesome-topic")
	if err != nil {
		log.Fatalf("\nFailed to create Pub/Sub topic. Reason - %+v\n", err)
	}

	_ = topic // TODO: use the topic.
}

// ExampleGoop_CreateSubscription - Example usage of the CreateSubscription function.
func ExampleGoop_CreateSubscription() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	g := &Goop{
		Context: ctx,
		Project: "project-id",
	}

	// Create the client.
	if err := g.CreateClient(); err != nil {
		log.Fatalf("\nFailed to create Pub/Sub client. Reason - %+v\n", err)
	}

	// Create the topic.
	topic, err := g.CreateTopic("my-awesome-topic")
	if err != nil {
		log.Fatalf("\nFailed to create Pub/Sub topic. Reason - %+v\n", err)
	}

	// Create the subscription.
	_, subErr := g.CreateSubscription(topic, "my-awesome-subscription")
	if subErr != nil {
		log.Fatalf("\nFailed to create Pub/Sub subscription. Reason - %+v\n", err)
	}
}

// ExampleGoop_PullMessages - Example usage of the PullMessages function.
func ExampleGoop_PullMessages() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	g := &Goop{
		Context: ctx,
		Project: "project-id",
	}

	// Callback function.
	myAwesomeCallback := func(msg *pubsub.Message, pubSub *Goop) error {
		fmt.Printf("Got messages %+v", msg)

		return nil
	}

	// Pull the messages.
	if err := g.PullMessages("my-awesome-subscription", myAwesomeCallback); err != nil {
		log.Fatalf("\nFailed to pull Pub/Sub messages. Reason - %+v\n", err)
	}
}

// ExampleGoop_Publish - Example usage of the Publish function.
func ExampleGoop_Publish() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	g := &Goop{
		Context: ctx,
		Project: "project-id",
	}

	// Publish the message.
	err := g.Publish("my-other-awesome-topic", "Hello world!")
	if err != nil {
		log.Fatalf("\nFailed to publish Pub/Sub messages. Reason - %+v\n", err)
	}
}

// ExampleGoop_PublishWithAttributes - Example usage of the PublishWithAttributes function.
func ExampleGoop_PublishWithAttributes() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	g := &Goop{
		Context: ctx,
		Project: "project-id",
	}

	// Make some attributes.
	messageAttributes := map[string]string{
		"answer_to_life_universe_everything": "42",
	}

	// Publish the message with attributes.
	err := g.PublishWithAttributes("my-other-awesome-topic", "Hello world!", messageAttributes)
	if err != nil {
		log.Fatalf("\nFailed to publish Pub/Sub messages. Reason - %+v\n", err)
	}
}
