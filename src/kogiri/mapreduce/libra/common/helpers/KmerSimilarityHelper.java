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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kogiri.mapreduce.libra.common.LibraConstants;
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
}
