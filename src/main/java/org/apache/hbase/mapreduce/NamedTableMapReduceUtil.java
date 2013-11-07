package org.apache.hbase.mapreduce;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: jibs
 */
public class NamedTableMapReduceUtil extends TableMapReduceUtil {

    /**
     * Use this before submitting a Multi TableMap job. It will appropriately set
     * up the job.
     *
     * @param scans The list of {@link org.apache.hadoop.hbase.client.Scan} objects to read from.
     * @param mapper The mapper class to use.
     * @param outputKeyClass The class of the output key.
     * @param outputValueClass The class of the output value.
     * @param job The current job to adjust. Make sure the passed job is carrying
     *          all necessary HBase configuration.
     * @param addDependencyJars upload HBase jars and jars for any of the
     *          configured job classes via the distributed cache (tmpjars).
     * @throws java.io.IOException When setting up the details fails.
     */
    public static void initTableMapperJob(List<Scan> scans,
                                          Class<? extends TableMapper> mapper,
                                          Class<? extends WritableComparable> outputKeyClass,
                                          Class<? extends Writable> outputValueClass, Job job,
                                          boolean addDependencyJars) throws IOException {
        job.setInputFormatClass(MultiTableInputFormat.class);
        if (outputValueClass != null) {
            job.setMapOutputValueClass(outputValueClass);
        }
        if (outputKeyClass != null) {
            job.setMapOutputKeyClass(outputKeyClass);
        }
        job.setMapperClass(mapper);
        HBaseConfiguration.addHbaseResources(job.getConfiguration());
        List<String> scanStrings = new ArrayList<String>();

        for (Scan scan : scans) {
            scanStrings.add(convertScanToString(scan));
        }
        job.getConfiguration().setStrings(MultiTableInputFormat.SCANS,
                scanStrings.toArray(new String[scanStrings.size()]));

        if (addDependencyJars) {
            addDependencyJars(job);
        }
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan  The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }


}
