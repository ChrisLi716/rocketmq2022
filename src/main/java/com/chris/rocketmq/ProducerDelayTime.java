package com.chris.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @author Chris
 * @date 2022-03-24 6:35 PM
 */
public class ProducerDelayTime {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException,
            InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("rocketmq-producer-group-001");
        producer.setNamesrvAddr("master:9876");

        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        System.out.println("producer has started!");

        int messageCount = 10;

        for (int i = 0; i < messageCount; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("topic-005" /* Topic */, "TagA" /* Tag */,
                    ("Hello DealyTime " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */);

            msg.setDelayTimeLevel(3);
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }

        producer.shutdown();
    }
}
