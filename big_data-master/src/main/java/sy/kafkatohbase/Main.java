package sy.kafkatohbase;

import java.io.IOException;

public class Main {
	public static void main(String[] args) throws IOException {  
		MyKafkaProducer prorun = new MyKafkaProducer();
		prorun.start();
		MyKafkaConsumer conrun1 = new MyKafkaConsumer();
		conrun1.start();
   }  
}
