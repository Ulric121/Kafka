package com.tao.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 同步发送生产者，生产环境很少使用
 *
 * @author tao
 */
public class SyncProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> first = producer.send(new ProducerRecord<>("first", "test-" + i));
            //get方法阻塞主线程，获取返回值，相当于join方法
            RecordMetadata recordMetadata = first.get();
            System.out.println(recordMetadata.toString());
        }

        producer.close();
    }
}
