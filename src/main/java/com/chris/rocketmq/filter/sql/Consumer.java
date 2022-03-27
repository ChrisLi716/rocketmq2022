package com.chris.rocketmq.filter.sql;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author Chris
 * @date 2022-03-24 6:40 PM
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rocketmq-consumer-group-003");

        // Specify name server addresses.
        consumer.setNamesrvAddr("master:9876");

        // only subsribe messages have property vip and number, also number between 3 and 7 and vip > 4
        consumer.subscribe("topic-006", MessageSelector.bySql("number between 3 and 7 and vip > 4"));

        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
                String msg_str = new String(msg.getBody());
                System.out.println(msg_str);

            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
