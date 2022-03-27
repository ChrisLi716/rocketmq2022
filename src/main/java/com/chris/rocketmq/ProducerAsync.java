package com.chris.rocketmq;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Chris
 * @date 2022-03-24 6:35 PM
 */
public class ProducerAsync {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException,
            InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("rocketmq-producer-group-001");
        producer.setNamesrvAddr("master:9876");

        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);

        System.out.println("producer has started!");

        int messageCount = 10;
        CountDownLatch countDownLatch = new CountDownLatch(messageCount);

        for (int i = 0; i < messageCount; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("topic-003" /* Topic */, "TagA" /* Tag */,
                    ("Hello Async " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */);


            //Send Messages Asynchronously
            //Asynchronous transmission is generally used in response time sensitive business scenarios.
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    countDownLatch.countDown();
                    System.out.printf("%s%n", sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    countDownLatch.countDown();
                    System.out.println("exception:" + e.getMessage());
                }
            });
        }

        countDownLatch.await();
        producer.shutdown();
    }
}
