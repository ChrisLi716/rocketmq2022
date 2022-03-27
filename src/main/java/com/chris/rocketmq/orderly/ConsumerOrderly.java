package com.chris.rocketmq.orderly;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author Chris
 * @date 2022-03-24 6:40 PM
 */
public class ConsumerOrderly {

    public static void main(String[] args) throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rocketmq-consumer-group-003");

        // Specify name server addresses.
        consumer.setNamesrvAddr("master:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("topic-008", "order");

        // Register MessageListenerOrderly to execute on arrival of messages fetched from brokers, receive messages
        // orderly One queue by one thread.
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                String msg_str = new String(msg.getBody());
                System.out.println(msg_str);
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
