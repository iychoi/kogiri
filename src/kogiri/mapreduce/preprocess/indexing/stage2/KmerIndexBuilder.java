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
package kogiri.mapreduce.preprocess.indexing.stage2;

import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.common.hadoop.io.format.fasta.FastaReadInputFormat;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.common.report.Report;
import kogiri.hadoop.common.cmdargs.CommandArgumentsParser;
import kogiri.mapreduce.common.helpers.MapReduceHelper;
import kogiri.mapreduce.preprocess.PreprocessorCmdArgs;
import kogiri.mapreduce.preprocess.common.IPreprocessorStage;
import kogiri.mapreduce.preprocess.common.PreprocessorConfig;
import kogiri.mapreduce.preprocess.common.PreprocessorConfigException;
import kogiri.mapreduce.preprocess.common.helpers.KmerHistogramHelper;
import kogiri.mapreduce.preprocess.common.kmerindex.KmerIndexIndex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilder extends Configured implements Tool, IPreprocessorStage {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerIndexBuilder(), args);
        System.exit(res);
    }
    
    public KmerIndexBuilder() {
        
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
        
        if(ppConfig.getKmerSize() <= 0) {
            throw new PreprocessorConfigException("invalid kmer size");
        }
        
        if(ppConfig.getReadIndexPath() == null) {
            throw new PreprocessorConfigException("cannot find read index path");
        }
        
        if(ppConfig.getKmerIndexPath() == null) {
            throw new PreprocessorConfigException("cannot find kmer index path");
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
        
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePath(conf, ppConfig.getFastaPath());
        
        boolean job_result = true;
        List<Job> jobs = new ArrayList<Job>();
        
        for(int round=0;round<inputFiles.length;round++) {
            Path roundInputFile = inputFiles[round];
            String roundOutputPath = ppConfig.getKmerIndexPath() + "_round" + round;
            
            Job job = new Job(conf, "Kogiri Preprocessor - Building Kmer Indices (" + round + " of " + inputFiles.length + ")");
            job.setJarByClass(KmerIndexBuilder.class);

            // Mapper
            job.setMapperClass(KmerIndexBuilderMapper.class);
            job.setInputFormatClass(FastaReadInputFormat.class);
            job.setMapOutputKeyClass(CompressedSequenceWritable.class);
            job.setMapOutputValueClass(CompressedIntArrayWritable.class);
            
            // Combiner
            job.setCombinerClass(KmerIndexBuilderCombiner.class);
            
            // Partitioner
            job.setPartitionerClass(KmerIndexBuilderPartitioner.class);
            
            // Reducer
            job.setReducerClass(KmerIndexBuilderReducer.class);

            // Specify key / value
            job.setOutputKeyClass(CompressedSequenceWritable.class);
            job.setOutputValueClass(CompressedIntArrayWritable.class);

            // Inputs
            FileInputFormat.addInputPaths(job, roundInputFile.toString());

            LOG.info("Input file : ");
            LOG.info("> " + roundInputFile.toString());
            
            String histogramFileName = KmerHistogramHelper.makeKmerHistogramFileName(roundInputFile.getName());
            Path histogramPath = new Path(ppConfig.getKmerHistogramPath(), histogramFileName);
            
            KmerIndexBuilderPartitioner.setHistogramPath(job.getConfiguration(), histogramPath);
            
            FileOutputFormat.setOutputPath(job, new Path(roundOutputPath));
            job.setOutputFormatClass(MapFileOutputFormat.class);
            
            // Use many reducers
            int reducersPerNode = ppConfig.getClusterConfiguration().getMachineCores() / 2;
            if(reducersPerNode < 1) {
                reducersPerNode = 1;
            }
            int reducers = ppConfig.getClusterConfiguration().getMachineNum() * (ppConfig.getClusterConfiguration().getMachineCores() / 2);
            LOG.info("Reducers : " + reducers);
            job.setNumReduceTasks(reducers);
            
            // Execute job and return status
            boolean result = job.waitForCompletion(true);
            
            jobs.add(job);

            // commit results
            if (result) {
                commitRoundIndexOutputFiles(roundInputFile, new Path(roundOutputPath), new Path(ppConfig.getKmerIndexPath()), job.getConfiguration(), ppConfig.getKmerSize());
                
                // create index of index
                createIndexOfIndex(new Path(ppConfig.getKmerIndexPath()), roundInputFile, job.getConfiguration(), ppConfig.getKmerSize());
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
    
    private void commitRoundIndexOutputFiles(Path roundInputPath, Path MROutputPath, Path finalOutputPath, Configuration conf, int kmerSize) throws IOException {
        FileSystem fs = MROutputPath.getFileSystem(conf);
        if(!fs.exists(finalOutputPath)) {
            fs.mkdirs(finalOutputPath);
        }
        
        FileStatus status = fs.getFileStatus(MROutputPath);
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(MROutputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    // rename outputs
                    int mapreduceID = MapReduceHelper.getMapReduceID(entryPath);
                    Path toPath = new Path(finalOutputPath, KmerIndexHelper.makeKmerIndexPartFileName(roundInputPath.getName(), kmerSize, mapreduceID));

                    LOG.info("output : " + entryPath.toString());
                    LOG.info("renamed to : " + toPath.toString());
                    fs.rename(entryPath, toPath);
                }
            }
        } else {
            throw new IOException("path not found : " + MROutputPath.toString());
        }
        
        fs.delete(MROutputPath, true);
    }
    
    private void createIndexOfIndex(Path indexPath, Path inputPath, Configuration conf, int kmerSize) throws IOException {
        String kmerIndexIndexFileName = KmerIndexHelper.makeKmerIndexIndexFileName(inputPath, kmerSize);
        Path kmerIndexIndexFilePath = new Path(indexPath, kmerIndexIndexFileName);
        Path[] indexFiles = KmerIndexHelper.getKmerIndexPartFilePath(conf, kmerIndexIndexFilePath);
        
        KmerIndexIndex indexIndex = new KmerIndexIndex();
        for(Path indexFile : indexFiles) {
            LOG.info("Reading the final key from " + indexFile.toString());
            MapFile.Reader reader = new MapFile.Reader(indexFile.getFileSystem(conf), indexFile.toString(), conf);
            CompressedSequenceWritable finalKey = new CompressedSequenceWritable();
            reader.finalKey(finalKey);
            indexIndex.addLastKey(finalKey.getSequence());
            reader.close();
        }

        LOG.info("Creating an index file : " + kmerIndexIndexFilePath.toString());
        indexIndex.saveTo(kmerIndexIndexFilePath.getFileSystem(conf), kmerIndexIndexFilePath);
    }
}
