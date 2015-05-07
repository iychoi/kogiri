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
package kogiri.common.hadoop.io.format.fasta;

import java.io.IOException;
import kogiri.common.fasta.FastaRead;
import kogiri.common.hadoop.io.reader.fasta.FastaReadReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author iychoi
 */
public class FastaReadInputFormat extends FileInputFormat<LongWritable, FastaRead> {

    private static final Log LOG = LogFactory.getLog(FastaReadInputFormat.class);
    
    private final static String CONF_SPLITABLE = "kogiri.comm.hadoop.io.format.fasta.splitable";
    
    @Override
    public RecordReader<LongWritable, FastaRead> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new FastaReadReader();
    }
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        boolean splitable = FastaReadInputFormat.isSplitable(context.getConfiguration());
        LOG.info("splitable = " + splitable);
        if(!splitable) {
            return false;
        }
        
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        if(codec != null) {
            return false;
        }
        
        return true;
    }
    
    public static void setSplitable(Configuration conf, boolean splitable) {
        conf.setBoolean(CONF_SPLITABLE, splitable);
    }
    
    public static boolean isSplitable(Configuration conf) {
        return conf.getBoolean(CONF_SPLITABLE, true);
    }
}