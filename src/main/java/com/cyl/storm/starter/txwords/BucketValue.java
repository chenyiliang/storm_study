package com.cyl.storm.starter.txwords;

import java.math.BigInteger;

public class BucketValue {
	private int count = 0;
	private BigInteger txid;

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public BigInteger getTxid() {
		return txid;
	}

	public void setTxid(BigInteger txid) {
		this.txid = txid;
	}

	@Override
	public String toString() {
		return "BucketValue [count=" + count + ", txid=" + txid + "]";
	}

}
