package com.tao.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * API简单消费者
 *
 * @author tao
 */
public class CustomConsumer {
    public static void main(String[] args) {

        //1.创建消费者配置信息
        Properties props = new Properties();
        //连接的集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //开启自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //自动提交的延迟
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //key、value的反序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "aaa");

        //2.创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //3.订阅主体
        consumer.subscribe(Arrays.asList("first", "second"));

        //一直进行循环，进行数据拉取
        while (true) {
            //4.获取数据，timeout是拉取到空数据时，等待的时间
            ConsumerRecords<String, String> records = consumer.poll(100);

            //5.解析并打印
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key() + "-" + record.value());
            }
        }
    }
}
