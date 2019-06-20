package sy.kafkatohbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MyKafkaConsumer extends Thread {
	
	public Properties properties;
	private static MyHbaseUtils hbUt;
	private Configuration conf;
	private static String tableName = "newgdsTable";
	private HTable table;
	
	//三个类簇分别为information、 ticket、 error
	private byte[] cfInforBytes = Bytes.toBytes("information");
	private byte[] cfTicketBytes = Bytes.toBytes("ticket");
	private byte[] cfErrorBytes = Bytes.toBytes("error");

	//14个列信息
	private byte[] colRowBytes = Bytes.toBytes("Rowkey");
	private byte[] colDtBytes = Bytes.toBytes("Dt");
	private byte[] colReqBytes = Bytes.toBytes("Req");
	private byte[] colResBytes = Bytes.toBytes("Res");
	private byte[] colIssuBytes = Bytes.toBytes("Issuccess");
	
	private byte[] colMacBytes = Bytes.toBytes("MAC");
	private byte[] colPcmBytes = Bytes.toBytes("PCM");
	private byte[] colAirpBytes = Bytes.toBytes("Airport");
	private byte[] colAirlBytes = Bytes.toBytes("Airline");
	private byte[] colAgentBytes = Bytes.toBytes("Agent");
	private byte[] colCountryBytes = Bytes.toBytes("Country");
	
	private byte[] colEsignBytes = Bytes.toBytes("Esign");
	private byte[] colEnumBytes = Bytes.toBytes("Enum");
	private byte[] colEtypeBytes = Bytes.toBytes("Etype");
	
	public MyKafkaConsumer() throws IOException
	{
		properties = new Properties();
		hbUt = new MyHbaseUtils();
		conf = hbUt.HBaseConnect();
		table = new HTable(conf,tableName);
	}
	public void consumerSetting()
	{
		//虚拟机ip
		properties.put("bootstrap.servers", "192.168.56.104:9092");
        //组名，不同的组名可以重复消费
		properties.put("group.id", "group-A");
		//是否自动提交，默认为true
        properties.put("enable.auto.commit", "true");
        //从poll(拉)的回话处理时长
        properties.put("auto.commit.interval.ms", "1000");
        //消费规则，默认earliest 
        properties.put("auto.offset.reset", "earliest");
        //超时时间
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	}
	@Override
	public void run() {
		consumerSetting();
		// TODO Auto-generated method stub
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
	    kafkaConsumer.subscribe(Arrays.asList("newGDS"));
	    while (true) {
	    	@SuppressWarnings("deprecation")
			ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
	        for (ConsumerRecord<String, String> record : records) {
	    		try {
	    			System.out.println("receiving:"+record.value());
	    			addRecord(record.value());
	    		} catch (Exception e) {
	    			// TODO Auto-generated catch block
	    			e.printStackTrace();
	    		}
	        }
	    }
	}
	
	public void addRecord(String str) throws Exception {
        @SuppressWarnings("resource")
        String[] strSplit = str.split("\\|");
        String rowKey = strSplit[0] + "-" + strSplit[1];
        
        //
        Put put = new Put(Bytes.toBytes(rowKey));// 指定行
        put.addColumn(cfInforBytes, colRowBytes, Bytes.toBytes(strSplit[0]));
        put.addColumn(cfInforBytes, colDtBytes, Bytes.toBytes(strSplit[1]));
        put.addColumn(cfInforBytes, colReqBytes, Bytes.toBytes(strSplit[8]));
        put.addColumn(cfInforBytes, colResBytes, Bytes.toBytes(strSplit[9]));
        put.addColumn(cfInforBytes, colIssuBytes, Bytes.toBytes(strSplit[13]));
       
        put.addColumn(cfTicketBytes, colMacBytes, Bytes.toBytes(strSplit[2]));
        put.addColumn(cfTicketBytes, colPcmBytes, Bytes.toBytes(strSplit[3]));
        put.addColumn(cfTicketBytes, colAirpBytes, Bytes.toBytes(strSplit[4]));
        put.addColumn(cfTicketBytes, colAirlBytes, Bytes.toBytes(strSplit[5]));
        put.addColumn(cfTicketBytes, colAgentBytes, Bytes.toBytes(strSplit[6]));
        put.addColumn(cfTicketBytes, colCountryBytes, Bytes.toBytes(strSplit[7]));
        
        put.addColumn(cfErrorBytes, colEsignBytes, Bytes.toBytes(strSplit[10]));
        put.addColumn(cfErrorBytes, colEnumBytes, Bytes.toBytes(strSplit[11]));
        put.addColumn(cfErrorBytes, colEtypeBytes, Bytes.toBytes(strSplit[12]));	  
        table.put(put);
        
        System.out.println("putting:"+ str);
        	   
}
	
	public static void main(String[] args) throws IOException {  
		System.out.println("1231234354455783");
		MyKafkaConsumer c = new MyKafkaConsumer();
		System.out.println("123123");
		String str ="6447014&48474545|20190423190000|17203550|CPHTL38AB|CPH||E|DK|1A|CA|S|0|\\N|1|0";
		try {
			c.addRecord(str);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   }  
}