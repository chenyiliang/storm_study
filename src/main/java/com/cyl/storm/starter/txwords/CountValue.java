package com.cyl.storm.starter.txwords;

import java.math.BigInteger;

public class CountValue {
	private Integer prev_count = null;
	private int count = 0;
	private BigInteger txid = null;

	public Integer getPrev_count() {
		return prev_count;
	}

	public void setPrev_count(Integer prev_count) {
		this.prev_count = prev_count;
	}

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
		return "CountValue [prev_count=" + prev_count + ", count=" + count
				+ ", txid=" + txid + "]";
	}

}
