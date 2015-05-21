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
package kogiri.mapreduce.preprocess.indexing.stage3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.common.report.Report;
import kogiri.mapreduce.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.preprocess.PreprocessorCmdArgs;
import kogiri.mapreduce.preprocess.common.IPreprocessStage;
import kogiri.mapreduce.preprocess.common.PreprocessorConfig;
import kogiri.mapreduce.preprocess.common.PreprocessorConfigException;
import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;
import kogiri.mapreduce.preprocess.common.helpers.KmerStatisticsHelper;
import kogiri.mapreduce.preprocess.common.kmerstatistics.KmerStatistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsBuilder extends Configured implements Tool, IPreprocessStage {
    
    private static final Log LOG = LogFactory.getLog(KmerStatisticsBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerStatisticsBuilder(), args);
        System.exit(res);
    }
    
    public KmerStatisticsBuilder() {
        
    }
    
    @Override
    public int run(String[] args) throws Exception {
        CommandArgumentsParser<PreprocessorCmdArgs> parser = new CommandArgumentsParser<PreprocessorCmdArgs>();
        PreprocessorCmdArgs cmdParams = new PreprocessorCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        PreprocessorConfig ppConfig = cmdParams.getPreprocessorConfig();
        
        return runJob(ppConfig);
    }
    
    @Override
    public int run(PreprocessorConfig ppConfig) throws Exception {
        setConf(new Configuration());
        return runJob(ppConfig);
    }
    
    private void validatePreprocessorConfig(PreprocessorConfig ppConfig) throws PreprocessorConfigException {
        if(ppConfig.getFastaPath().size() <= 0) {
            throw new PreprocessorConfigException("cannot find input sample path");
        }
        
        if(ppConfig.getClusterConfiguration() == null) {
            throw new PreprocessorConfigException("cannout find cluster configuration");
        }
        
        if(ppConfig.getKmerHistogramPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer histogram path");
        }
        
        if(ppConfig.getKmerIndexPath() == null) {
            throw new PreprocessorConfigException("cannot find k-mer index path");
        }
    }
    
    private int runJob(PreprocessorConfig ppConfig) throws Exception {
        // check config
        validatePreprocessorConfig(ppConfig);
        
        // configuration
        Configuration conf = this.getConf();
        
        // set user configuration
        ppConfig.getClusterConfiguration().configureTo(conf);
        ppConfig.saveTo(conf);
        
        Path[] inputFiles = KmerIndexHelper.getAllKmerIndexIndexFilePath(conf, ppConfig.getKmerIndexPath());
        
        for(Path inputFile : inputFiles) {
            LOG.info(inputFile);
        }
        
        boolean job_result = true;
        List<Job> jobs = new ArrayList<Job>();
        
        for(int round=0;round<inputFiles.length;round++) {
            Path roundInputFile = inputFiles[round];
            Path[] roundInputKmerIndexPartFiles = KmerIndexHelper.getKmerIndexPartFilePath(conf, roundInputFile);
            
            Job job = new Job(conf, "Kogiri Preprocessor - Computing Kmer Statistics (" + round + " of " + inputFiles.length + ")");
            job.setJarByClass(KmerStatisticsBuilder.class);
            
            // Mapper
            job.setMapperClass(KmerStatisticsBuilderMapper.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(NullWritable.class);

            // Specify key / value
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);

            // Inputs
            Path[] kmerIndexPartDataFiles = KmerIndexHelper.getAllKmerIndexPartDataFilePath(conf, roundInputKmerIndexPartFiles);
            SequenceFileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(kmerIndexPartDataFiles));

            LOG.info("Input file : ");
            LOG.info("> " + roundInputFile.toString());
            
            // Outputs
            job.setOutputFormatClass(NullOutputFormat.class);

            job.setNumReduceTasks(0);

            // Execute job and return status
            boolean result = job.waitForCompletion(true);

            jobs.add(job);

            // check results
            if(result) {
                CounterGroup uniqueGroup = job.getCounters().getGroup(KmerStatisticsHelper.getCounterGroupNameUnique());
                CounterGroup totalGroup = job.getCounters().getGroup(KmerStatisticsHelper.getCounterGroupNameTotal());
                CounterGroup squareGroup = job.getCounters().getGroup(KmerStatisticsHelper.getCounterGroupNameSquare());

                Iterator<Counter> uniqueIterator = uniqueGroup.iterator();
                while(uniqueIterator.hasNext()) {
                    long count = 0;
                    long length = 0;
                    long square = 0;
                    double real_mean = 0;
                    double stddev = 0;

                    Counter uniqueCounter = uniqueIterator.next();
                    Counter totalCounter = totalGroup.findCounter(uniqueCounter.getName());
                    Counter squareCounter = squareGroup.findCounter(uniqueCounter.getName());

                    count = uniqueCounter.getValue();
                    length = totalCounter.getValue();
                    square = squareCounter.getValue();

                    real_mean = (double)length / (double)count;
                    // stddev = sqrt((sum(lengths ^ 2)/count) - (mean ^ 2)
                    double mean = Math.pow(real_mean, 2);
                    double term = (double)square / (double)count;
                    stddev = Math.sqrt(term - mean);

                    LOG.info("distinct k-mers " + uniqueCounter.getName() + " : " + count);
                    LOG.info("total k-mers " + uniqueCounter.getName() + " : " + length);
                    LOG.info("average " + uniqueCounter.getName() + " : " + real_mean);
                    LOG.info("std-deviation " + uniqueCounter.getName() + " : " + stddev);

                    Path outputHadoopPath = new Path(ppConfig.getStatisticsPath(), KmerStatisticsHelper.makeKmerStatisticsFileName(uniqueCounter.getName()));
                    FileSystem fs = outputHadoopPath.getFileSystem(conf);

                    KmerStatistics statistics = new KmerStatistics();
                    statistics.setSampleName(uniqueCounter.getName());
                    statistics.setKmerSize(ppConfig.getKmerSize());
                    statistics.setUniqueKmers(count);
                    statistics.setTotalKmers(length);
                    statistics.setAverageFrequency(real_mean);
                    statistics.setStdDeviation(stddev);

                    statistics.saveTo(fs, outputHadoopPath);
                }
            }
            
            if(!result) {
                LOG.error("job failed at round " + round + " of " + inputFiles.length);
                job_result = false;
                break;
            }
        }
        
        // report
        if(ppConfig.getReportPath() != null && !ppConfig.getReportPath().isEmpty()) {
            Report report = new Report();
            report.addJob(jobs);
            report.writeTo(ppConfig.getReportPath());
        }
        
        return job_result ? 0 : 1;
    }
}
