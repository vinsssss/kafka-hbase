package sy.kafkatohbase;

import java.io.IOException;

public class Main {
	public static void main(String[] args) throws IOException {  
		MyKafkaProducer prorun = new MyKafkaProducer();
		prorun.start();
		MyKafkaConsumer conrun = new MyKafkaConsumer();
		conrun.start();
   }  
}
