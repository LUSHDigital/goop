package goop

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
)

// ExamplePubSub_CreateClient - Example usage of the CreateClient function.
func ExamplePubSub_CreateClient() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	pubSubWrapper := &PubSub{
		Context: ctx,
		Project: "project-id",
	}

	// Create the client.
	if err := pubSubWrapper.CreateClient(); err != nil {
		log.Fatalf("\nFailed to create Pub/Sub client. Reason - %+v\n", err)
	}
}

// ExamplePubSub_CreateTopic - Example usage of the CreateTopic function.
func ExamplePubSub_CreateTopic() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	pubSubWrapper := &PubSub{
		Context: ctx,
		Project: "project-id",
	}

	// Create the client.
	if err := pubSubWrapper.CreateClient(); err != nil {
		log.Fatalf("\nFailed to create Pub/Sub client. Reason - %+v\n", err)
	}

	// Create the topic.
	topic, err := pubSubWrapper.CreateTopic("my-awesome-topic")
	if err != nil {
		log.Fatalf("\nFailed to create Pub/Sub topic. Reason - %+v\n", err)
	}

	_ = topic // TODO: use the topic.
}

// ExamplePubSub_CreateSubscription - Example usage of the CreateSubscription function.
func ExamplePubSub_CreateSubscription() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	pubSubWrapper := &PubSub{
		Context: ctx,
		Project: "project-id",
	}

	// Create the client.
	if err := pubSubWrapper.CreateClient(); err != nil {
		log.Fatalf("\nFailed to create Pub/Sub client. Reason - %+v\n", err)
	}

	// Create the topic.
	topic, err := pubSubWrapper.CreateTopic("my-awesome-topic")
	if err != nil {
		log.Fatalf("\nFailed to create Pub/Sub topic. Reason - %+v\n", err)
	}

	// Create the subscription.
	_, subErr := pubSubWrapper.CreateSubscription(topic, "my-awesome-subscription")
	if subErr != nil {
		log.Fatalf("\nFailed to create Pub/Sub subscription. Reason - %+v\n", err)
	}
}

// ExamplePubSub_PullMessages - Example usage of the PullMessages function.
func ExamplePubSub_PullMessages() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	pubSubWrapper := &PubSub{
		Context: ctx,
		Project: "project-id",
	}

	// Callback function.
	myAwesomeCallback := func(msg *pubsub.Message, pubSub *PubSub) error {
		fmt.Printf("Got messages %+v", msg)

		return nil
	}

	// Pull the messages.
	if err := pubSubWrapper.PullMessages("my-awesome-subscription", myAwesomeCallback); err != nil {
		log.Fatalf("\nFailed to pull Pub/Sub messages. Reason - %+v\n", err)
	}
}

// ExamplePubSub_Publish - Example usage of the Publish function.
func ExamplePubSub_Publish() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	pubSubWrapper := &PubSub{
		Context: ctx,
		Project: "project-id",
	}

	// Publish the message.
	err := pubSubWrapper.Publish("my-other-awesome-topic", "Hello world!")
	if err != nil {
		log.Fatalf("\nFailed to publish Pub/Sub messages. Reason - %+v\n", err)
	}
}

// ExamplePubSub_PublishWithAttributes - Example usage of the PublishWithAttributes function.
func ExamplePubSub_PublishWithAttributes() {
	ctx := context.Background()

	// Create Pub/Sub wrapper.
	pubSubWrapper := &PubSub{
		Context: ctx,
		Project: "project-id",
	}

	// Make some attributes.
	messageAttributes := map[string]string{
		"answer_to_life_universe_everything": "42",
	}

	// Publish the message with attributes.
	err := pubSubWrapper.PublishWithAttributes("my-other-awesome-topic", "Hello world!", messageAttributes)
	if err != nil {
		log.Fatalf("\nFailed to publish Pub/Sub messages. Reason - %+v\n", err)
	}
}
