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
package kogiri.mapreduce.preprocess.common.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kogiri.common.helpers.FileSystemHelper;
import kogiri.mapreduce.preprocess.common.PreprocessorConstants;
import kogiri.mapreduce.preprocess.common.readindex.ReadIndexPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class ReadIndexHelper {
    
    private final static String READ_INDEX_PATH_EXP = ".+\\." + PreprocessorConstants.READ_INDEX_FILENAME_EXTENSION + "$";
    private final static Pattern READ_INDEX_PATH_PATTERN = Pattern.compile(READ_INDEX_PATH_EXP);
    
    public static String makeReadIndexFileName(String filename) {
        return filename + "." + PreprocessorConstants.READ_INDEX_FILENAME_EXTENSION;
    }
    
    public static String getSampleFileName(String readIndexFileName) {
        int idx = readIndexFileName.lastIndexOf("." + PreprocessorConstants.READ_INDEX_FILENAME_EXTENSION);
        if(idx > 0) {
            return readIndexFileName.substring(0, idx);
        }
        return readIndexFileName;
    }
    
    public static boolean isReadIndexFile(Path path) {
        return isReadIndexFile(path.getName());
    }
    
    public static boolean isReadIndexFile(String path) {
        Matcher matcher = READ_INDEX_PATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static Path[] getAllReadIndexFilePath(Configuration conf, String[] inputPaths) throws IOException {
        return getAllReadIDIndexFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllReadIndexFilePath(Configuration conf, Collection<String> inputPaths) throws IOException {
        return getAllReadIDIndexFilePath(conf, FileSystemHelper.makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllReadIDIndexFilePath(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        ReadIndexPathFilter filter = new ReadIndexPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                if(status.isDir()) {
                    if(filter.accept(path)) {
                        inputFiles.add(path);
                    } else {
                        // check child
                        FileStatus[] entries = fs.listStatus(path);
                        for (FileStatus entry : entries) {
                            if(entry.isDir()) {
                                if (filter.accept(entry.getPath())) {
                                    inputFiles.add(entry.getPath());
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
}
