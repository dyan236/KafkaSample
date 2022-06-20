import java.util.Properties;
import org.apache.kafka.clients.producer.*;

public class SampleKafkaProducer {

	public static void main(String[] args) {
		System.out.println("Entry");
		if(args.length==0) {
			System.out.println("Usage: <topic-name>");
			return;
		}
		String topicName=args[0].toString();
		Properties props=new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> p=new KafkaProducer<>(props);
		for(int i=0;i<10;i++) {
			p.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
		}
		System.out.println("Message sent");
		p.close();
	}

}
