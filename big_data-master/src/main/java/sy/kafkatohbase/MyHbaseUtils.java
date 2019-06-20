package sy.kafkatohbase;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class MyHbaseUtils {
	
	public Configuration HBaseConnect()
	{
		Configuration conf= HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","192.168.56.104,192.168.56.102,192.168.56.105");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	    conf.set("hbase.master", "192.168.56.104:60010");
	    return conf;
	}
	
}
