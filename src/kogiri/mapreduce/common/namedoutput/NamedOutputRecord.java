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
package kogiri.mapreduce.common.namedoutput;

import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class NamedOutputRecord implements Comparable<NamedOutputRecord> {
    private String identifier;
    private String filename;
    
    public NamedOutputRecord() {
    }
    
    public NamedOutputRecord(Path file) {
        initialize(NamedOutputs.getSafeIdentifier(file.getName()), file.getName());
    }
    
    public NamedOutputRecord(String identifier, Path file) {
        initialize(identifier, file.getName());
    }
    
    public NamedOutputRecord(String filename) {
        initialize(NamedOutputs.getSafeIdentifier(filename), filename);
    }
    
    public NamedOutputRecord(String identifier, String filename) {
        initialize(identifier, filename);
    }
    
    @JsonIgnore
    private void initialize(String identifier, String filename) {
        this.identifier = identifier;
        this.filename = filename;
    }
    
    @JsonProperty("identifier")
    public String getIdentifier() {
        return this.identifier;
    }
    
    @JsonProperty("identifier")
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
    
    @JsonProperty("filename")
    public String getFilename() {
        return this.filename;
    }
    
    @JsonProperty("filename")
    public void setFilename(String filename) {
        this.filename = filename;
    }
    
    @JsonIgnore
    @Override
    public int compareTo(NamedOutputRecord right) {
        return this.identifier.compareToIgnoreCase(right.identifier);
    }
}
