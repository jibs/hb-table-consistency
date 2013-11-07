package com.github.jibs.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.mapreduce.Job
import java.util
import org.apache.hadoop.io.Text

/**
 * User: jibs
 */
class JobRunner {
  def main(args: Array[String]) {
    def main(args: Array[String]) {

      val config: Configuration = HBaseConfiguration.create
      val job: Job = new Job(config, "table-consistency")
      job.setJarByClass(classOf[JobRunner])

      val scannerList: util.List[Scan] = new util.ArrayList[Scan]()
      
      val sourceScan: Scan = new Scan
      sourceScan.setCaching(500)
      sourceScan.setCacheBlocks(false)
      sourceScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "sourcetable".getBytes)
      scannerList.add(sourceScan)

      val sinkScan: Scan = new Scan
      sinkScan.setCaching(500)
      sinkScan.setCacheBlocks(false)
      sinkScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "sinktable".getBytes)
      scannerList.add(sinkScan)

      TableMapReduceUtil.initTableMapperJob(scannerList, classOf[SourceMapper], classOf[Text], classOf[Result], job)

      TableMapReduceUtil.initTableReducerJob(targetTable, null, job)

      job.setNumReduceTasks(0)
      val b: Boolean = job.waitForCompletion(true)

      if (!b) {
        throw new Nothing("error with job!")
      }
    }
  }
}
