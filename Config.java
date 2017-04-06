package edu.util.config;

import java.io.IOException;

import javax.naming.NamingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

@org.springframework.context.annotation.Configuration
@PropertySource(value="classpath:application.properties")
public class Config {

	@Autowired
	@Value("#{'${HBASE_ZOOKEEPER_QUORUM}'}")
	private String hbaseZooKeeperQuorum;

	@Autowired
	@Value("#{'${HBASE_ZOOKEEPER_QUORUM_VALUE}'}")
	private String hbaseZooKeeperQuorumValue;

	@Autowired
	@Value("#{'${HBASE_ZOOKEEPER_CLIENTPORT}'}")
	private String hbaseZooKeeperClientPort;

	@Autowired
	@Value("#{'${HBASE_ZOOKEEPER_CLIENTPORT_VALUE}'}")
	private String hbaseZooKeeperClientPortValue;

	@Autowired
	@Value("#{'${HBASE_CLUSTER_DISTRIBUTED}'}")
	private String hbaseClusterDistributed;

	@Autowired
	@Value("#{'${HBASE_CLUSTER_DISTRIBUTED_VALUE}'}")
	private String hbaseClusterDistributedValue;

	@Autowired
	@Value("#{'${ZOOKEEPER_ZNODE_PARENT}'}")
	private String zookeeperZnodeParent;

	@Autowired
	@Value("#{'${ZOOKEEPER_ZNODE_PARENT_VALUE}'}")
	private String zookeeperZnodeParentValue;


	@Bean(name = "hbaseTemplate")
	public HbaseTemplate createHbaseTemplate() throws IOException, NamingException {
		
		Configuration hbaseConfiguration = new Configuration();
		hbaseConfiguration.set(hbaseZooKeeperQuorum, hbaseZooKeeperQuorumValue);
		hbaseConfiguration.set(hbaseZooKeeperClientPort, hbaseZooKeeperClientPortValue);
		hbaseConfiguration.set(hbaseClusterDistributed, hbaseClusterDistributedValue);
		hbaseConfiguration.set(zookeeperZnodeParent, zookeeperZnodeParentValue);
		
		// create template
		HbaseTemplate hbaseTemplate = new HbaseTemplate(HBaseConfiguration.create(hbaseConfiguration));
		return hbaseTemplate;
	}
}
