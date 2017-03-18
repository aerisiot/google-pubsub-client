package com.aeris.pubsub;

import java.io.IOException;
import java.util.concurrent.Executor;

import org.apache.tomcat.jni.Time;

import com.google.api.gax.core.RpcFuture;
import com.google.api.gax.core.RpcFutureCallback;
import com.google.cloud.pubsub.spi.v1.AckReply;
import com.google.cloud.pubsub.spi.v1.AckReplyConsumer;
import com.google.cloud.pubsub.spi.v1.MessageReceiver;
import com.google.cloud.pubsub.spi.v1.Publisher;
import com.google.cloud.pubsub.spi.v1.Subscriber;
import com.google.cloud.pubsub.spi.v1.SubscriberClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;


public class Echo {
    private static final String PUBSUB_MT_TOPIC_NAME= "to-gateway";
    private static final String PUBSUB_MO_TOPIC_NAME= "t6mo";
    private static final String PUBSUB_MO_SUBSCRIPTION = "t6mo-app";
    private static final String MQTT_TOPIC = "mqtt_topic";
    private static final String MQTT_MO_TOPIC_PREFIX = "T6MO/";
    private static final String MQTT_MT_TOPIC_PREFIX = "T6MT/";
    
    private final String projectId = "your-project-id";
    
    private final Publisher publisher;
    private final Subscriber subscriber;
    
    public Echo() throws IOException {
        // make a REST client
        // client = Client.create();
        
        // make producer
        TopicName topic = TopicName.create(projectId, PUBSUB_MT_TOPIC_NAME);
        publisher = Publisher.newBuilder(topic).build();
        
        // make consumer
        try {
            createSubscription();
        } catch (Exception e) {
            // exception would happen when the target subscription already exists.
            e.printStackTrace();
        }
        SubscriptionName subscription = SubscriptionName.create(projectId, PUBSUB_MO_SUBSCRIPTION);
        MessageReceiver receiver = new MessageReceiver() {
            @Override
            public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                handleMessage(message);
                consumer.accept(AckReply.ACK, null);
            }
        };
        subscriber = Subscriber.newBuilder(subscription, receiver).build();
        
        Executor executor = new Executor() {
            @Override
            public void execute(Runnable command) {
                // TODO Auto-generated method stub
            }
        };

        subscriber.addListener(new Subscriber.SubscriberListener() {
            @Override
            public void failed(Subscriber.State from, Throwable failure) {
                // TODO Handle error.
            }
        }, executor);
    }
    
    // make sure that the subscription for the topic PUBSUB_MO_TOPIC_NAME exists
    private void createSubscription() throws Exception {
        try (SubscriberClient subscriberClient = SubscriberClient.create()) {
            SubscriptionName name = SubscriptionName.create(projectId, PUBSUB_MO_SUBSCRIPTION);
            TopicName topic = TopicName.create(projectId, PUBSUB_MO_TOPIC_NAME);
            PushConfig pushConfig = PushConfig.newBuilder().build();
            int ackDeadlineSeconds = 0;
            subscriberClient.createSubscription(name, topic, pushConfig, ackDeadlineSeconds);
        }   
    }
    
    private void handleMessage(PubsubMessage message) {
        // TODO this is a synchronous message handling
        System.out.println(message);
        String mqtt_topic = message.getAttributesMap().get(MQTT_TOPIC);
        if (mqtt_topic == null || !mqtt_topic.startsWith(MQTT_MO_TOPIC_PREFIX)) {
            return;
        }
        String topic = mqtt_topic.substring(MQTT_MO_TOPIC_PREFIX.length()).toLowerCase();
        if (topic.equals("echo")) {
            echo(message.getData());
        }
    }
    
    private void echo(ByteString data) {
        ByteString dataToSend = ByteString.copyFromUtf8("received: ").concat(data);
        sendMT(dataToSend, "echo");
        // String mqtt_topic = MQTT_MT_TOPIC_PREFIX + "echo";
    }
    
    private void sendMT(ByteString data, String localTopic) {
        String mqtt_topic = MQTT_MT_TOPIC_PREFIX + localTopic;
        
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data)
                .putAttributes("topic", mqtt_topic).build();
        RpcFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        messageIdFuture.addCallback(new RpcFutureCallback<String>() {
            @Override
            public void onSuccess(String messageId) {
                System.out.println("published with message id: " + messageId);
            }

            @Override
            public void onFailure(Throwable t) {
                System.out.println("failed to publish: " + t);
            }
        });
    }
    
    public void run() {

        subscriber.startAsync();
        while (true) {
            Time.sleep(10000);
        }
        // subscriber.stopAsync().awaitTerminated();
    }

    public static void main(String[] args) throws IOException, Exception {
        Echo echo = new Echo();
        echo.run();
    }

}
