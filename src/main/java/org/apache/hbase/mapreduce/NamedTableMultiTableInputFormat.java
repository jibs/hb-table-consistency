package org.apache.hbase.mapreduce;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Date: 07/11/13
 */
public class NamedTableMultiTableInputFormat extends MultiTableInputFormat {
    @Override
    public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        TableSplit tSplit = (TableSplit) split;

        context.getConfiguration().set("hbase.mapreduce.multitable.tablename", Bytes.toString(tSplit.getTableName()));
        return super.createRecordReader(split, context);
    }
}
