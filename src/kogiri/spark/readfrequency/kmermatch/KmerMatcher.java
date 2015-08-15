/*
 * Copyright (C) 2015 iychoi
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package kogiri.spark.readfrequency.kmermatch;

import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.hadoop.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;
import kogiri.mapreduce.preprocess.common.kmerindex.KmerIndexInputFormat;
import kogiri.mapreduce.preprocess.common.kmerindex.KmerIndexInputFormatConfig;
import kogiri.spark.readfrequency.ReadFrequencyCounterCmdArgs;
import kogiri.spark.readfrequency.common.ReadFrequencyCounterConfig;
import kogiri.spark.readfrequency.common.ReadFrequencyCounterConfigException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author iychoi
 */
public class KmerMatcher {
    
    private static final Log LOG = LogFactory.getLog(KmerMatcher.class);
    
    private static final int PARTITIONS_PER_CORE = 10;
    
    public static void main(String[] args) throws Exception {
        int res = run(args);
        System.exit(res);
    }
    
    public static int run(String[] args) throws Exception {
        CommandArgumentsParser<ReadFrequencyCounterCmdArgs> parser = new CommandArgumentsParser<ReadFrequencyCounterCmdArgs>();
        ReadFrequencyCounterCmdArgs cmdParams = new ReadFrequencyCounterCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        ReadFrequencyCounterConfig rfConfig = cmdParams.getReadFrequencyCounterConfig();
        
        return runJob(rfConfig);
    }
    
    public static int run(ReadFrequencyCounterConfig rfConfig) throws Exception {
        return runJob(rfConfig);
    }
    
    private static void validateReadFrequencyCounterConfig(ReadFrequencyCounterConfig rfConfig) throws ReadFrequencyCounterConfigException {
        if(rfConfig.getKmerIndexPath().size() <= 0) {
            throw new ReadFrequencyCounterConfigException("cannot find input kmer index path");
        }
        
        if(rfConfig.getClusterConfiguration() == null) {
            throw new ReadFrequencyCounterConfigException("cannout find cluster configuration");
        }
        
        if(rfConfig.getKmerHistogramPath() == null) {
            throw new ReadFrequencyCounterConfigException("cannot find kmer histogram path");
        }
        
        if(rfConfig.getKmerStatisticsPath() == null) {
            throw new ReadFrequencyCounterConfigException("cannot find kmer statistics path");
        }
        
        if(rfConfig.getKmerMatchPath() == null) {
            throw new ReadFrequencyCounterConfigException("cannot find kmer match path");
        }
    }
    
    private static int runJob(ReadFrequencyCounterConfig rfConfig) throws Exception {
        // check config
        validateReadFrequencyCounterConfig(rfConfig);
        
        // configuration
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Kogiri Read Frequency Counter - Finding Matching Kmers");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        
        // set user configuration
        rfConfig.getClusterConfiguration().configureTo(sparkConf);
        rfConfig.saveTo(sparkConf);
        
        // Inputs
        Path[] kmerIndexFiles = KmerIndexHelper.getAllKmerIndexIndexFilePath(sparkContext.hadoopConfiguration(), rfConfig.getKmerIndexPath());
        
        int kmerSize = 0;
        for(Path inputFile : kmerIndexFiles) {
            // check kmerSize
            int myKmerSize = KmerIndexHelper.getKmerSize(inputFile);
            if(kmerSize == 0) {
                kmerSize = myKmerSize;
            } else {
                if(kmerSize != myKmerSize) {
                    throw new Exception("kmer size must be the same over all given kmer indices");
                }
            }
        }
        
        for(Path kmerIndexFile : kmerIndexFiles) {
            Path[] kmerIndexPartFilePath = KmerIndexHelper.getKmerIndexPartFilePath(sparkContext.hadoopConfiguration(), kmerIndexFile);
            
            KmerIndexInputFormatConfig inputFormatConfig = new KmerIndexInputFormatConfig();
            inputFormatConfig.setKmerSize(kmerSize);
            inputFormatConfig.setKmerIndexIndexPath(kmerIndexFile.toString());
            KmerIndexInputFormat.setInputFormatConfig(sparkContext.hadoopConfiguration(), inputFormatConfig);
            
            JavaPairRDD<CompressedSequenceWritable, CompressedIntArrayWritable> kmerIndexRDD = sparkContext.newAPIHadoopFile(FileSystemHelper.makeCommaSeparated(kmerIndexPartFilePath), KmerIndexInputFormat.class, CompressedSequenceWritable.class, CompressedIntArrayWritable.class, sparkContext.hadoopConfiguration());
            //JavaPairRDD<String, int[]> indexRDD = kmerIndexRDD.mapToPair(new KmerIndexToStringMapper());
            Path dumpPath = new Path(rfConfig.getKmerMatchPath(), kmerIndexFile.getName() + ".dump");
            kmerIndexRDD.saveAsTextFile(dumpPath.toString());
        }
        
        /*
        JavaPairRDD<LongWritable, FastaRead> inputRDD = sparkContext.newAPIHadoopFile(inputFile.toString(), FastaReadInputFormat.class, LongWritable.class, FastaRead.class, sparkContext.hadoopConfiguration());
        JavaPairRDD<Tuple2<LongWritable, FastaRead>, Long> indexedRDD = inputRDD.zipWithIndex();

        JavaPairRDD<Long, Integer> ridxRDD = indexedRDD.mapToPair(new ReadIndexBuilderMapper());

        String ridxFilename = ReadIndexHelper.makeReadIndexFileName(inputFile.getName());
        Path ridxPath = new Path(ppConfig.getReadIndexPath(), ridxFilename);
        ridxRDD.saveAsTextFile(ridxPath.toString());
        */
        
        /*
        // Mapper
        KmerMatcherFileMapping fileMapping = new KmerMatcherFileMapping();
        for(Path kmerIndexFile : kmerIndexFiles) {
            String fastaFilename = KmerIndexHelper.getFastaFileName(kmerIndexFile.getName());
            fileMapping.addFastaFile(fastaFilename);
        }
        fileMapping.saveTo(conf);
        
        int MRNodes = MapReduceClusterHelper.getNodeNum(conf);
        
        LOG.info("MapReduce nodes detected : " + MRNodes);

        KmerMatchInputFormatConfig matchInputFormatConfig = new KmerMatchInputFormatConfig();
        matchInputFormatConfig.setKmerSize(kmerSize);
        matchInputFormatConfig.setPartitionNum(MRNodes * PARTITIONS_PER_CORE);
        matchInputFormatConfig.setKmerHistogramPath(rfConfig.getKmerHistogramPath());
        matchInputFormatConfig.setKmerStatisticsPath(rfConfig.getKmerStatisticsPath());
        matchInputFormatConfig.setStandardDeviationFactor(rfConfig.getStandardDeviationFactor());
        
        KmerMatchInputFormat.setInputFormatConfig(job, matchInputFormatConfig);
        
        FileOutputFormat.setOutputPath(job, new Path(rfConfig.getKmerMatchPath()));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Map only job
        job.setNumReduceTasks(0);

        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        if(result) {
            commit(new Path(rfConfig.getKmerMatchPath()), conf);
            
            Path tableFilePath = new Path(rfConfig.getKmerMatchPath(), KmerMatchHelper.makeKmerMatchTableFileName());
            FileSystem fs = tableFilePath.getFileSystem(conf);
            fileMapping.saveTo(fs, tableFilePath);
        }
        
        // report
        if(rfConfig.getReportPath() != null && !rfConfig.getReportPath().isEmpty()) {
            Report report = new Report();
            report.addJob(job);
            report.writeTo(rfConfig.getReportPath());
        }
        
        return result ? 0 : 1;
        */
        return 0;
    }
    
    /*
    private void commit(Path outputPath, Configuration conf) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        
        FileStatus status = fs.getFileStatus(outputPath);
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(outputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    // rename outputs
                    int mapreduceID = MapReduceHelper.getMapReduceID(entryPath);
                    String newName = KmerMatchHelper.makeKmerMatchResultFileName(mapreduceID);
                    Path toPath = new Path(entryPath.getParent(), newName);

                    LOG.info("output : " + entryPath.toString());
                    LOG.info("renamed to : " + toPath.toString());
                    fs.rename(entryPath, toPath);
                } else {
                    // let it be
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
    }
    */

    private static class KmerIndexToStringMapper implements PairFunction<Tuple2<CompressedSequenceWritable, CompressedIntArrayWritable>, String, int[]> {

        @Override
        public Tuple2<String, int[]> call(Tuple2<CompressedSequenceWritable, CompressedIntArrayWritable> t) throws Exception {
            return new Tuple2<String, int[]>(t._1.toString(), t._2.get());
        }
    }
}
