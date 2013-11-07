package com.github.jibs.hbase

import org.apache.hadoop.hbase.mapreduce.TableMapper
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.mapreduce.{TaskAttemptContext, Mapper}
import org.apache.hadoop.io.Text

/**
 */
class SourceMapper extends TableMapper[Text, Result] {
  var text = new Text()

  override def setup(context: Mapper#Context) = {
    context.getConfiguration
  }

  override def map(key: ImmutableBytesWritable, value: Result, context: Context) = {
    value.
  }
}
