package com.quovantis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements WritableComparable<CompositeKey> {
	
	private String text;
	private Long hits;
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public Long getHits() {
		return hits;
	}
	public void setHits(Long hits) {
		this.hits = hits;
	}
	
	public CompositeKey(String text, Long hits) {
		super();
		this.text = text;
		this.hits = hits;
	}
	
	
	

	public CompositeKey() {
	}
	public void readFields(DataInput arg0) throws IOException {
		this.text = WritableUtils.readString(arg0);
		this.hits = WritableUtils.readVLong(arg0);
		
	}

	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeString(arg0, text);
		WritableUtils.writeVLong(arg0, hits);
		
	}

	public int compareTo(CompositeKey o) {
		// TODO Auto-generated method stub
		int result =  this.hits.compareTo(o.getHits());
		if(result == 0) {
			result = this.text.compareTo(o.getText());
		}
		return result;
	}

}
