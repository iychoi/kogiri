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

import kogiri.mapreduce.preprocess.common.PreprocessorConstants;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsHelper {
    private final static String COUNTER_GROUP_NAME_UNIQUE = "KmerStatisticsUnique";
    private final static String COUNTER_GROUP_NAME_TOTAL = "KmerStatisticsTotal";
    private final static String COUNTER_GROUP_NAME_SQUARE = "KmerStatisticsSquare";
    
    public static String makeKmerStatisticsFileName(Path filePath) {
        return makeKmerStatisticsFileName(filePath.getName());
    }
    
    public static String makeKmerStatisticsFileName(String filename) {
        return filename + "." + PreprocessorConstants.KMER_STATISTICS_FILENAME_EXTENSION;
    }
    
    public static String getCounterGroupNameUnique() {
        return COUNTER_GROUP_NAME_UNIQUE;
    }
    
    public static String getCounterGroupNameTotal() {
        return COUNTER_GROUP_NAME_TOTAL;
    }

    public static String getCounterGroupNameSquare() {
        return COUNTER_GROUP_NAME_SQUARE;
    }
}
