package com.chris.rocketmq.orderly;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import com.chris.rocketmq.Bean.OrderInfo;
import com.sun.media.jfxmedia.logging.Logger;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Chris
 * @date 2022-03-24 6:35 PM
 */
public class ProducerOrderly {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException,
            InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("rocketmq-producer-group-002");
        producer.setNamesrvAddr("master:9876");

        producer.start();
        System.out.println("producer has started!");

        List<OrderInfo> orderInfos = getOrderInfos();
        for (OrderInfo orderInfo : orderInfos) {
            Message message = new Message("topic-008", "order", orderInfo.toString().getBytes(StandardCharsets.UTF_8));
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    System.out.println("message queue selector, arg:" + arg);
                    int size = mqs.size();
                    long id = (long) arg;
                    long queue_inx = id % size;
                    return mqs.get(new Long(queue_inx).intValue());
                }
            }, orderInfo.getId(), 10000);
            System.out.println("send result:" + sendResult);
        }

        //Shut down once the producer instance is not longer in use.
        producer.shutdown();

    }


    private static List<OrderInfo> getOrderInfos() {
        List<OrderInfo> orderInfos = CollUtil.newArrayList();
        orderInfos.add(new OrderInfo(1L, "create"));
        orderInfos.add(new OrderInfo(2L, "create"));
        orderInfos.add(new OrderInfo(1L, "send"));
        orderInfos.add(new OrderInfo(3L, "create"));
        orderInfos.add(new OrderInfo(2L, "send"));
        orderInfos.add(new OrderInfo(4L, "create"));
        orderInfos.add(new OrderInfo(1L, "pay"));
        orderInfos.add(new OrderInfo(1L, "finish"));
        orderInfos.add(new OrderInfo(3L, "send"));
        orderInfos.add(new OrderInfo(2L, "pay"));
        orderInfos.add(new OrderInfo(4L, "send"));
        orderInfos.add(new OrderInfo(3L, "pay"));
        orderInfos.add(new OrderInfo(2L, "finish"));
        orderInfos.add(new OrderInfo(4L, "pay"));
        orderInfos.add(new OrderInfo(3L, "finish"));
        orderInfos.add(new OrderInfo(4L, "finish"));
        return orderInfos;
    }

}
