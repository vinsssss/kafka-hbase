package sy.kafkatohbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyKafkaProducer extends Thread {
	
	public String filepath = "C:\\Users\\89252\\Desktop\\大数据\\大数据技术结课实验20190520(终)\\结课实验数据_GDS订票请求日志数据.txt";
	public  Properties properties = new Properties();
	public void producerSetting()
	{
        //虚拟机IP
        properties.put("bootstrap.servers", "192.168.56.121:9092,192.168.56.122:9092,192.168.56.123:9092");
        //所有follower都响应了才认为消息提交成功，即"committed"
        properties.put("acks", "all");
        properties.put("retries", 0);
        //producer将试图批处理消息记录，以减少请求次数.默认的批量处理消息字节数
        properties.put("batch.size", 16384);
        //延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        properties.put("linger.ms", 1);
        //producer用来缓存数据的内存大小(设置为3MB)
        properties.put("buffer.memory", 3145728);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
	}

	@Override
	public void run() {
		producerSetting();
		// TODO Auto-generated method stub
		Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
            FileReader fr = new FileReader(filepath);
			BufferedReader bf = new BufferedReader(fr);
			String str;
			int i=0;
			while ((str = bf.readLine()) != null) {             
				System.out.println("sending: "+ i +str);
                producer.send(new ProducerRecord<String, String>("GDS1", str));         
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
	}
}