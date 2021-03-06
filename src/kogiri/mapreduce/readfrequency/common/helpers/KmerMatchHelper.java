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

package kogiri.mapreduce.readfrequency.common.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.mapreduce.readfrequency.common.ReadFrequencyCounterConstants;
import kogiri.mapreduce.readfrequency.common.kmermatch.KmerMatchResultPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerMatchHelper {
    private final static String KMER_MATCH_RESULT_PATH_EXP = ".+\\." + ReadFrequencyCounterConstants.KMER_MATCH_RESULT_FILENAME_EXTENSION + "\\.\\d+$";
    private final static Pattern KMER_MATCH_RESULT_PATH_PATTERN = Pattern.compile(KMER_MATCH_RESULT_PATH_EXP);
    
    public static boolean isKmerMatchTableFile(Path path) {
        return isKmerMatchTableFile(path.getName());
    }
    
    public static boolean isKmerMatchTableFile(String path) {
        if(path.compareToIgnoreCase(ReadFrequencyCounterConstants.KMER_MATCH_TABLE_FILENAME) == 0) {
            return true;
        }
        return false;
    }
    
    public static boolean isKmerMatchResultFile(Path path) {
        return isKmerMatchResultFile(path.getName());
    }
    
    public static boolean isKmerMatchResultFile(String path) {
        Matcher matcher = KMER_MATCH_RESULT_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static String makeKmerMatchTableFileName() {
        return ReadFrequencyCounterConstants.KMER_MATCH_TABLE_FILENAME;
    }
    
    public static String makeKmerMatchResultFileName(int mapreduceID) {
        return ReadFrequencyCounterConstants.KMER_MATCH_RESULT_FILENAME_PREFIX + "." + ReadFrequencyCounterConstants.KMER_MATCH_RESULT_FILENAME_EXTENSION + "." + mapreduceID;
    }
    
    public static int getMatchResultPartID(Path matchResultFilePath) {
        return getMatchResultPartID(matchResultFilePath.getName());
    }
    
    public static int getMatchResultPartID(String matchResultFileName) {
        int idx = matchResultFileName.lastIndexOf(".");
        if(idx >= 0) {
            String partID = matchResultFileName.substring(idx + 1);
            return Integer.parseInt(partID);
        }
        return -1;
    }
    
    public static Path[] getAllKmerMatchResultFilePath(Configuration conf, String inputPathsCommaSeparated) throws IOException {
        return getAllKmerMatchResultFilePath(conf, FileSystemHelper.makePathFromString(conf, FileSystemHelper.splitCommaSeparated(inputPathsCommaSeparated)));
    }
    
    public static Path[] getAllKmerMatchResultFilePath(Configuration conf, String[] inputPath) throws IOException {
        return getAllKmerMatchResultFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPath));
    }
    
    public static Path[] getAllKmerMatchResultFilePath(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerMatchResultPathFilter filter = new KmerMatchResultPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                if(status.isDir()) {
                    // check child
                    FileStatus[] entries = fs.listStatus(path);
                    for (FileStatus entry : entries) {
                        if(!entry.isDir()) {
                            if (filter.accept(entry.getPath())) {
                                inputFiles.add(entry.getPath());
                            }
                        }
                    }
                } else {
                    if (filter.accept(status.getPath())) {
                        inputFiles.add(status.getPath());
                    }
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
}
