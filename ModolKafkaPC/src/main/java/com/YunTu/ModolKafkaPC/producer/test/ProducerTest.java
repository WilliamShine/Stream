package com.YunTu.ModolKafkaPC.producer.test;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {

    public static void main(String[] args) {
        Properties props = new Properties();//47.100.9.7,47.100.9.241,47.100.6.154
        props.put("bootstrap.servers", "47.100.9.7:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String,String>(props);

        for(int i=0;i<100;i++){
            ProducerRecord<String,String> r = new ProducerRecord<String,String>("sytest","key-"+i,"value-"+i);
            producer.send(r);
            System.out.println(i);
        }

        producer.close();
    }

}


/*package com.YunTu.KafkaTest.producer.test;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.javaapi.producer.Producer;

public class ProducerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.100.9.241:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(props);
        for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord("sytest1", Integer.toString(i), Integer.toString(i)));
            System.out.println(i);
        }
        producer.close();
    }
}
*/

/*package com.YunTu.KafkaTest.producer.test;

import java.util.Properties;

import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {

	public static void main(String[] args) {

		Properties props = new Properties();

		props.put("bootstrap.servers", "47.100.9.22:9092");

		props.put("acks", "all");

		props.put("retries", 0);

		props.put("batch.size", 16384);

		props.put("linger.ms", 1);

		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer producer = new KafkaProducer<>(props);

		boolean flag = true;

		Scanner input = new Scanner(System.in);

		do {

			System.out.println("请输入key和value的格式");

			producer.send(new ProducerRecord("test4", input.nextLine(), input.nextLine()));

		} while (flag);

		producer.close();
		for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord("shequ", Integer.toString(i), Integer.toString(i)));
            System.out.println(i);
        }
        producer.close();
	}

}*/

/*package com.YunTu.KafkaTest.producer.test;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "47.100.9.241:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(props);
		for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord("shequ", Integer.toString(i), Integer.toString(i)));
            System.out.println(i);
        }
		producer.close();
	}
}*/