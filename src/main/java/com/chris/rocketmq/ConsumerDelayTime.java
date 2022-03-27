package com.chris.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * @author Chris
 * @date 2022-03-24 6:40 PM
 */
public class ConsumerDelayTime {

    public static void main(String[] args) throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rocketmq-consumer-group-003");

        // Specify name server addresses.
        consumer.setNamesrvAddr("master:9876");

        // Subscribe one more more topics to consume.
        consumer.subscribe("topic-005", "*");

        // 默认早集群模式,如果改为广播模式，同组的不同消费将会消费到生产者生产的全部消息
        consumer.setMessageModel(MessageModel.BROADCASTING);

        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);
                String msg_str = new String(msg.getBody());
                // Print approximate delay time period
                System.out.println("Receive message[msgId=" + msg.getMsgId() + ",msg_str=] " + msg_str + "," + (System.currentTimeMillis() - msg.getStoreTimestamp()) + "ms later");


            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
