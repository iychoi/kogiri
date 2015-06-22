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
package kogiri.mapreduce.readfrequency.modecount;

import java.io.IOException;
import java.util.Collection;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.MultiFileIntWritable;
import kogiri.mapreduce.readfrequency.common.kmermatch.KmerMatchOutputRecord;
import kogiri.mapreduce.readfrequency.common.kmermatch.KmerMatchOutputRecordField;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class ModeCounterMapper extends Mapper<LongWritable, Text, MultiFileIntWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(ModeCounterMapper.class);
    
    private ModeCounterConfig modeCounterConfig = null;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.modeCounterConfig = ModeCounterConfig.createInstance(conf);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        int tabIndex = line.indexOf("\t");
        if(tabIndex > 0) {
            String json = line.substring(tabIndex+1);
            
            KmerMatchOutputRecord record = KmerMatchOutputRecord.createInstance(json);
        
            if(record.getFields().size() <= 1) {
                throw new IOException("Number of match output field must be larger than 1");
            }

            KmerMatchOutputRecordField masterField = null;
            Collection<KmerMatchOutputRecordField> fields = record.getFields();
            for(KmerMatchOutputRecordField field : fields) {
                if(field.getFileID() == this.modeCounterConfig.getMasterFileID()) {
                    masterField = field;
                    break;
                }
            }

            KmerMatchOutputRecordField[] fieldArray = fields.toArray(new KmerMatchOutputRecordField[0]);

            if(masterField != null) {
                int[] count_pos_vals = new int[fieldArray.length];
                int[] count_neg_vals = new int[fieldArray.length];

                for(int i=0;i<fields.size();i++) {
                    Collection<Integer> readIDs = fieldArray[i].getReadIDs();
                    Integer[] readIDArray = readIDs.toArray(new Integer[0]);

                    int pos = 0;
                    int neg = 0;
                    for(int j=0;j<readIDArray.length;j++) {
                        if(readIDArray[j] >= 0) {
                            pos++;
                        } else {
                            neg++;
                        }
                    }

                    count_pos_vals[i] = pos;
                    count_neg_vals[i] = neg;
                }

                Collection<Integer> masterReadIDs = masterField.getReadIDs();
                Integer[] masterReadIDArray = masterReadIDs.toArray(new Integer[0]);

                for(int i=0;i<masterReadIDArray.length;i++) {
                    int readID = masterReadIDArray[i];

                    for(int j=0;j<fieldArray.length;j++) {
                        if(fieldArray[j].getFileID() != masterField.getFileID()) {
                            int fileID = fieldArray[j].getFileID();
                            if(readID >= 0) {
                                // pos
                                if(count_pos_vals[j] > 0) {
                                    // forward match
                                    int[] ciaw_val = new int[2];
                                    ciaw_val[0] = count_pos_vals[j];
                                    ciaw_val[1] = 1;
                                    CompressedIntArrayWritable ciaw = new CompressedIntArrayWritable(ciaw_val);
                                    context.write(new MultiFileIntWritable(fileID, readID), ciaw);
                                }

                                if(count_neg_vals[j] > 0) {
                                    // reverse match
                                    int[] ciaw_val = new int[2];
                                    ciaw_val[0] = -1 * count_neg_vals[j];
                                    ciaw_val[1] = 1;
                                    CompressedIntArrayWritable ciaw = new CompressedIntArrayWritable(ciaw_val);
                                    context.write(new MultiFileIntWritable(fileID, readID), ciaw);
                                }
                            } else {
                                // neg
                                readID *= -1;
                                if(count_pos_vals[j] > 0) {
                                    // reverse match
                                    int[] ciaw_val = new int[2];
                                    ciaw_val[0] = -1 * count_pos_vals[j];
                                    ciaw_val[1] = 1;
                                    CompressedIntArrayWritable ciaw = new CompressedIntArrayWritable(ciaw_val);
                                    context.write(new MultiFileIntWritable(fileID, readID), ciaw);
                                }

                                if(count_neg_vals[j] > 0) {
                                    // forward match
                                    int[] ciaw_val = new int[2];
                                    ciaw_val[0] = count_neg_vals[j];
                                    ciaw_val[1] = 1;
                                    CompressedIntArrayWritable ciaw = new CompressedIntArrayWritable(ciaw_val);
                                    context.write(new MultiFileIntWritable(fileID, readID), ciaw);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.modeCounterConfig = null;
    }
}
