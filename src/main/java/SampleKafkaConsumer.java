import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;

public class SampleKafkaConsumer {

	public static void main(String[] args) {
		if(args.length==0) {
			System.out.println("Usage: <topic-name>");
			return;
		}
		String topicName=args[0].toString();
		Properties props=new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("auto.commit.interval.ms", 1000);
		props.put("session.timeout.ms", 30000);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> c=new KafkaConsumer<>(props);
		c.subscribe(Arrays.asList(topicName));
		System.out.println("Subscribed to "+topicName);
		//int i=0;
		while(true) {
			ConsumerRecords<String, String> records=c.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> r:records)
				System.out.printf("offset=%d, key=%s, value=%s\n", r.offset(), r.key(), r.value());
		}
	}

}
