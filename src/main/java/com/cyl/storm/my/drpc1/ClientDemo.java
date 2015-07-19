package com.cyl.storm.my.drpc1;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class ClientDemo {

	public static void main(String[] args) throws TException, DRPCExecutionException {
		DRPCClient client = new DRPCClient("drpc-host", 3772); 
		String result = client.execute("reach", "http://twitter.com");
		

	}

}
