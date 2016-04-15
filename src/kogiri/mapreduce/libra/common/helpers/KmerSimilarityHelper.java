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

package kogiri.mapreduce.libra.common.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.mapreduce.libra.common.LibraConstants;
import kogiri.mapreduce.libra.common.kmersimilarity.KmerSimilarityResultPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityHelper {
    private final static String KMER_SIMILARITY_RESULT_PATH_EXP = ".+\\." + LibraConstants.KMER_SIMILARITY_RESULT_FILENAME_EXTENSION + "\\.\\d+$";
    private final static Pattern KMER_SIMILARITY_RESULT_PATH_PATTERN = Pattern.compile(KMER_SIMILARITY_RESULT_PATH_EXP);
    
    public static boolean isKmerSimilarityTableFile(Path path) {
        return isKmerSimilarityTableFile(path.getName());
    }
    
    public static boolean isKmerSimilarityTableFile(String path) {
        if(path.compareToIgnoreCase(LibraConstants.KMER_SIMILARITY_TABLE_FILENAME) == 0) {
            return true;
        }
        return false;
    }
    
    public static boolean isKmerSimilarityResultFile(Path path) {
        return isKmerSimilarityResultFile(path.getName());
    }
    
    public static boolean isKmerSimilarityResultFile(String path) {
        Matcher matcher = KMER_SIMILARITY_RESULT_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static String makeKmerSimilarityTableFileName() {
        return LibraConstants.KMER_SIMILARITY_TABLE_FILENAME;
    }
    
    public static String makeKmerSimilarityResultFileName(int mapreduceID) {
        return LibraConstants.KMER_SIMILARITY_RESULT_FILENAME_PREFIX + "." + LibraConstants.KMER_SIMILARITY_RESULT_FILENAME_EXTENSION + "." + mapreduceID;
    }
    
    public static String makeKmerSimilarityFinalResultFileName() {
        return LibraConstants.KMER_SIMILARITY_RESULT_FILENAME_PREFIX + "." + LibraConstants.KMER_SIMILARITY_RESULT_FILENAME_EXTENSION;
    }
    
    public static Path[] getAllKmerSimilarityResultFilePath(Configuration conf, String inputPathsCommaSeparated) throws IOException {
        return getAllKmerSimilarityResultFilePath(conf, FileSystemHelper.makePathFromString(conf, FileSystemHelper.splitCommaSeparated(inputPathsCommaSeparated)));
    }
    
    public static Path[] getAllKmerSimilarityResultFilePath(Configuration conf, String[] inputPath) throws IOException {
        return getAllKmerSimilarityResultFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPath));
    }
    
    public static Path[] getAllKmerSimilarityResultFilePath(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerSimilarityResultPathFilter filter = new KmerSimilarityResultPathFilter();
        
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
