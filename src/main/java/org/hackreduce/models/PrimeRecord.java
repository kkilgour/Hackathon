package org.hackreduce.models;

import org.apache.hadoop.io.Text;

public class PrimeRecord {

	long num;

	public PrimeRecord(Text text) throws IllegalArgumentException {
		this(text.toString());
	}
	
	public PrimeRecord(String inputString) throws IllegalArgumentException {
		String[] attributes = inputString.split(",");
		try {
			setNum(Long.parseLong(attributes[0]));
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed", e);
		}
		
	}

	public long getNum() {
		return num;
	}

	public void setNum(long value) {
		this.num = value;
	}
}
