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
package kogiri.spark.preprocess.indexing.stage1;

import kogiri.common.fasta.FastaRead;
import kogiri.common.hadoop.io.format.fasta.FastaReadInputFormat;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.hadoop.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.preprocess.common.helpers.ReadIndexHelper;
import kogiri.spark.preprocess.PreprocessorCmdArgs;
import kogiri.spark.preprocess.common.PreprocessorConfig;
import kogiri.spark.preprocess.common.PreprocessorConfigException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author iychoi
 */
public class ReadIndexBuilder {
    
    private static final Log LOG = LogFactory.getLog(ReadIndexBuilder.class);
    
    private SparkConf sparkConf;
    
    public static void main(String[] args) throws Exception {
        int res = run(args);
        System.exit(res);
    }
    
    
    private SparkConf getSparkConf() {
        return this.sparkConf;
    }
    
    public static int run(String[] args) throws Exception {
        CommandArgumentsParser<PreprocessorCmdArgs> parser = new CommandArgumentsParser<PreprocessorCmdArgs>();
        PreprocessorCmdArgs cmdParams = new PreprocessorCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        PreprocessorConfig ppConfig = cmdParams.getPreprocessorConfig();
        
        return runJob(ppConfig);
    }
    
    public static int run(PreprocessorConfig ppConfig) throws Exception {
        return runJob(ppConfig);
    }
    
    private static void validatePreprocessorConfig(PreprocessorConfig ppConfig) throws PreprocessorConfigException {
        if(ppConfig.getFastaPath().size() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getClusterConfiguration() == null) {
            throw new PreprocessorConfigException("cannout find cluster configuration");
        }
        
        if(ppConfig.getKmerHistogramPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer histogram path");
        }
        
        if(ppConfig.getKmerSize() <= 0) {
            throw new PreprocessorConfigException("invalid kmer size");
        }
        
        if(ppConfig.getReadIndexPath() == null) {
            throw new PreprocessorConfigException("cannot find read index path");
        }
    }
    
    private static int runJob(PreprocessorConfig ppConfig) throws Exception {
        // check config
        validatePreprocessorConfig(ppConfig);
        
        // configuration
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Kogiri Preprocessor - Building Read Indices");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        
        // set user configuration
        ppConfig.getClusterConfiguration().configureTo(sparkConf);
        ppConfig.saveTo(sparkConf);
        
        // inputs
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePath(sparkContext.hadoopConfiguration(), ppConfig.getFastaPath());
        for(Path inputFile : inputFiles) {
            JavaPairRDD<LongWritable, FastaRead> inputRDD = sparkContext.newAPIHadoopFile(inputFile.toString(), FastaReadInputFormat.class, LongWritable.class, FastaRead.class, sparkContext.hadoopConfiguration());
            JavaPairRDD<Tuple2<LongWritable, FastaRead>, Long> indexedRDD = inputRDD.zipWithIndex();
            
            JavaPairRDD<Long, Integer> ridxRDD = indexedRDD.mapToPair(new ReadIndexBuilderMapper());
            
            String ridxFilename = ReadIndexHelper.makeReadIndexFileName(inputFile.getName());
            Path ridxPath = new Path(ppConfig.getReadIndexPath(), ridxFilename);
            ridxRDD.saveAsTextFile(ridxPath.toString());
        }
        
        
        /*
        // commit results
        if(result) {
            commit(new Path(ppConfig.getReadIndexPath()), sparkConf, namedOutputs);
            commit(new Path(ppConfig.getKmerHistogramPath()), sparkConf, namedOutputs);
        }
        
        // report
        if(ppConfig.getReportPath() != null && !ppConfig.getReportPath().isEmpty()) {
            Report report = new Report();
            report.addJob(job);
            report.writeTo(ppConfig.getReportPath());
        }
        
        return result ? 0 : 1;
        */
        return 0;
    }
    
    private static class ReadIndexBuilderMapper implements PairFunction<scala.Tuple2<scala.Tuple2<LongWritable, FastaRead>, Long>, Long, Integer> {
        @Override
        public scala.Tuple2<Long, Integer> call(scala.Tuple2<scala.Tuple2<LongWritable, FastaRead>, Long> t) throws Exception {
            // readid starts from 1
            long readid = t._2() + 1;
            scala.Tuple2<LongWritable, FastaRead> pair = t._1();
            FastaRead read = pair._2();

            return new scala.Tuple2<Long, Integer>(read.getReadOffset(), (int)readid);
        }
    }
    
    /*
    private void commit(Path outputPath, Configuration conf, NamedOutputs namedOutputs) throws IOException {
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
                    fs.delete(entryPath, true);
                } else if(KmerHistogramHelper.isKmerHistogramFile(entryPath)) {
                    // not necessary
                } else {
                    // rename outputs
                    NamedOutputRecord namedOutput = namedOutputs.getRecordFromMROutput(entryPath.getName());
                    if(namedOutput != null) {
                        Path toPath = new Path(entryPath.getParent(), ReadIndexHelper.makeReadIndexFileName(namedOutput.getFilename()));
                        
                        LOG.info("output : " + entryPath.toString());
                        LOG.info("renamed to : " + toPath.toString());
                        fs.rename(entryPath, toPath);
                    }
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
    }
    */
}
