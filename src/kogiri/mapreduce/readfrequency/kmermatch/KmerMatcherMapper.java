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
package kogiri.mapreduce.readfrequency.kmermatch;

import kogiri.mapreduce.common.kmermatch.KmerMatchFileMapping;
import kogiri.mapreduce.common.kmermatch.KmerMatchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import kogiri.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import kogiri.common.hadoop.io.datatypes.CompressedSequenceWritable;
import kogiri.common.json.JsonSerializer;
import kogiri.mapreduce.preprocess.common.helpers.KmerIndexHelper;
import kogiri.mapreduce.readfrequency.common.kmermatch.KmerMatchOutputRecord;
import kogiri.mapreduce.readfrequency.common.kmermatch.KmerMatchOutputRecordField;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class KmerMatcherMapper extends Mapper<CompressedSequenceWritable, KmerMatchResult, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(KmerMatcherMapper.class);
    
    private KmerMatchFileMapping fileMapping;
    private Hashtable<String, Integer> idCacheTable;
    private Counter reportCounter;
    private JsonSerializer serializer;
    
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.fileMapping = KmerMatchFileMapping.createInstance(context.getConfiguration());
        
        this.idCacheTable = new Hashtable<String, Integer>();
        
        this.reportCounter = context.getCounter("KmerMatcher", "report");
        
        this.serializer = new JsonSerializer();
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, KmerMatchResult value, Context context) throws IOException, InterruptedException {
        CompressedIntArrayWritable[] valueArray = value.getVals();
        Path[] kmerIndexPathArray = value.getKmerIndexPath();
        
        // filter out empty values
        ArrayList<CompressedIntArrayWritable> filteredValueArray = new ArrayList<CompressedIntArrayWritable>();
        ArrayList<Path> filteredKmerIndexPathArray = new ArrayList<Path>();
        
        for(int i=0;i<valueArray.length;i++) {
            if(!valueArray[i].isEmpty()) {
                filteredValueArray.add(valueArray[i]);
                filteredKmerIndexPathArray.add(kmerIndexPathArray[i]);
            }
        }
        
        valueArray = null;
        kmerIndexPathArray = null;
        
        if(filteredValueArray.size() <= 1) {
            // skip
            return;
        }
        
        int[] fileid_arr = new int[filteredValueArray.size()];
        
        for(int i=0;i<filteredValueArray.size();i++) {
            int fileidInt = 0;
            String indexFilename = filteredKmerIndexPathArray.get(i).getName();
            Integer fileid = this.idCacheTable.get(indexFilename);
            if(fileid == null) {
                String fastaFilename = KmerIndexHelper.getFastaFileName(indexFilename);
                int id = this.fileMapping.getIDFromFastaFile(fastaFilename);
                this.idCacheTable.put(indexFilename, id);
                fileidInt = id;
            } else {
                fileidInt = fileid.intValue();
            }
            
            fileid_arr[i] = fileidInt;
        }
        
        KmerMatchOutputRecord outputRecord = new KmerMatchOutputRecord();
        
        for(int i=0;i<fileid_arr.length;i++) {
            KmerMatchOutputRecordField field = new KmerMatchOutputRecordField();
            field.setFileID(fileid_arr[i]);
            field.addReadIDs(filteredValueArray.get(i).get());
            
            outputRecord.addField(field);
        }
        
        this.reportCounter.increment(1);
        
        String json = this.serializer.toJson(outputRecord);
        
        context.write(new Text(key.getSequence()), new Text(json));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.fileMapping = null;
        this.idCacheTable.clear();
        this.idCacheTable = null;
        this.serializer = null;
    }
}
