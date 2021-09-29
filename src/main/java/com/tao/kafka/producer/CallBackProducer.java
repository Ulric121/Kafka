package com.tao.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 带回调函数的生产者
 *
 * @author tao
 */
public class CallBackProducer {
    public static void main(String[] args) {
        //1.定义配置信息
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //2.创建生产者对象
        Producer<String, String> producer = new KafkaProducer<>(props);

        //3.发送数据
        for (int i = 0; i < 10; i++) {
            //回调函数，该方法会在Producer收到ack时调用，为异步调用
            producer.send(new ProducerRecord<>("first", "test-" + i),
                    (metadata, exception) -> {
                        if (exception == null) {
                            System.out.println(metadata.partition() + "-" + metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    });
        }

        //4.关闭资源
        producer.close();
    }
}
