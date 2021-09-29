package com.tao.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 调用分区器的生产者
 *
 * @author tao
 */
public class PartitionProducer {
    public static void main(String[] args) {
        //1.定义配置信息
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //添加自定义的partitioner
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.tao.kafka.partitioner.MyPartitioner");

        //2.创建生产者对象
        Producer<String, String> producer = new KafkaProducer<>(props);

        //3.发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "test-" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + "-" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }

        //4.关闭资源
        producer.close();
    }

}
