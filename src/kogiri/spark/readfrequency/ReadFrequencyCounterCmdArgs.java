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
package kogiri.spark.readfrequency;

import java.util.ArrayList;
import java.util.List;
import kogiri.hadoop.common.cmdargs.CommandArgumentsBase;
import kogiri.spark.readfrequency.common.ReadFrequencyCounterConfig;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class ReadFrequencyCounterCmdArgs extends CommandArgumentsBase {
    
    public ReadFrequencyCounterCmdArgs() {
        
    }
    
    @Option(name = "--stddev_factor", usage = "specify standard deviation factor")
    protected double stddevFactor = ReadFrequencyCounterConfig.DEFAULT_STANDARD_DEVIATION_FACTOR;
    
    public double getStandardDeviationFactor() {
        return this.stddevFactor;
    }
    
    @Option(name = "--histogram", usage = "specify kmer histogram path")
    protected String kmerHistogramPath;

    public String getKmerHistogramPath() {
        return this.kmerHistogramPath;
    }
    
    @Option(name = "--statistics", usage = "specify kmer statistics path")
    protected String kmerStatisticsPath;
    
    public String getKmerStatisticsPath() {
        return this.kmerStatisticsPath;
    }
    
    @Option(name = "-o", usage = "specify output path")
    private String outputPath = ReadFrequencyCounterConfig.DEFAULT_OUTPUT_ROOT_PATH;
        
    public String getOutputPath() {
        return this.outputPath;
    }
    
    @Argument(metaVar = "input-path [input-path ...]", usage = "input-paths")
    private List<String> inputPaths = new ArrayList<String>();

    public String[] getInputPaths() {
        if(this.inputPaths.isEmpty()) {
            return new String[0];
        }

        return this.inputPaths.toArray(new String[0]);
    }

    public String getCommaSeparatedInputPath() {
        String[] inputPaths = getInputPaths();
        StringBuilder CSInputPath = new StringBuilder();
        for(String inputpath : inputPaths) {
            if(CSInputPath.length() != 0) {
                CSInputPath.append(",");
            }
            CSInputPath.append(inputpath);
        }
        return CSInputPath.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(String arg : this.inputPaths) {
            if(sb.length() != 0) {
                sb.append(", ");
            }

            sb.append(arg);
        }

        return "paths = " + sb.toString();
    }

    @Override
    public boolean checkValidity() {
        if(!super.checkValidity()) {
           return false;
        }
        
        if(this.kmerHistogramPath == null ||
                this.kmerStatisticsPath == null ||
                this.outputPath == null ||
                this.inputPaths == null || 
                this.inputPaths.isEmpty() || 
                this.inputPaths.size() < 1) {
            return false;
        }
        
        return true;
    }
    
    public ReadFrequencyCounterConfig getReadFrequencyCounterConfig() {
        ReadFrequencyCounterConfig config = new ReadFrequencyCounterConfig();
        
        config.setReportPath(this.reportfile);
        config.setClusterConfiguration(this.cluster);
        config.addKmerIndexPath(this.inputPaths);
        config.setKmerHistogramPath(this.kmerHistogramPath);
        config.setKmerStatisticsPath(this.kmerStatisticsPath);
        config.setStandardDeviationFactor(this.stddevFactor);
        config.setOutputRootPath(this.outputPath);
        return config;
    }
}
