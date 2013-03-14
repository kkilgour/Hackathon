package org.hackreduce.models;

import org.apache.hadoop.io.Text;

public class PrimeRecord {

	int num;

	public PrimeRecord(Text text) throws IllegalArgumentException {
		this(text.toString());
	}
	
	public PrimeRecord(String inputString) throws IllegalArgumentException {
		String[] attributes = inputString.split(",");
		try {
			setNum(Integer.parseInt(attributes[0]));
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed", e);
		}
		
	}

	public int getNum() {
		return num;
	}

	public void setNum(int value) {
		this.num = value;
	}
}
