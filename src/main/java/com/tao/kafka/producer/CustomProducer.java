package com.tao.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 普通生产者
 *
 * @author tao
 */
public class CustomProducer {
    public static void main(String[] args) {
        //1.定义配置信息
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2.创建生产者对象
        Producer<String, String> producer = new KafkaProducer<>(props);

        //3.发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "test-" + i));
        }

        //4.关闭资源
        producer.close();
    }

}
