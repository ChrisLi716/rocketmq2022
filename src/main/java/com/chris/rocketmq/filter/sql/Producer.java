package com.chris.rocketmq.filter.sql;

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
public class Producer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException,
            InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("rocketmq-producer-group-001");
        producer.setNamesrvAddr("master:9876");

        producer.start();
        System.out.println("producer has started!");

        for (int i = 0; i < 10; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("topic-006" /* Topic */, "vip" /* Tag */,
                    ("Hello vip " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */);

            msg.putUserProperty("vip", String.valueOf(i));
            msg.putUserProperty("number", String.valueOf(i));

            //Call send message to deliver message to one of brokers.
            //发送同步消息
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();

    }
}
