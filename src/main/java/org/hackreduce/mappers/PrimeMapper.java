package org.hackreduce.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.hackreduce.models.PrimeRecord;


/**
 * Extends the basic Hadoop {@link Mapper} to process the prime dump by
 * accessing {@link PrimeRecord}
 *
 * @param <K> Output class of the mapper key
 * @param <V> Output class of the mapper value
 *
 */
public abstract class PrimeMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<PrimeRecord, LongWritable, Text, K, V> {

	/**
	 * Configures the MapReduce job to read data from the Freebase quad dump.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	@Override
	protected PrimeRecord instantiateModel(LongWritable key, Text value) {
		return new PrimeRecord(value);
	}

}