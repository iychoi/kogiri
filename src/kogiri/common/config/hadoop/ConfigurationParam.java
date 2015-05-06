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
package kogiri.common.config.hadoop;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class ConfigurationParam {
    private String key;
    private String value;
    
    public ConfigurationParam() {
        
    }
    
    public ConfigurationParam(String key, String value) {
        this.key = key;
        this.value = value;
    }
    
    @JsonProperty("key")
    public void setKey(String key) {
        this.key = key;
    }
    
    @JsonProperty("key")
    public String getKey() {
        return this.key;
    }
    
    @JsonProperty("value")
    public void setValue(String value) {
        this.value = value;
    }
    
    @JsonProperty("value")
    public String getValue() {
        return this.value;
    }
    
    @JsonIgnore
    public int getValueAsInt() {
        return Integer.parseInt(this.value);
    }
    
    @JsonIgnore
    @Override
    public String toString() {
        return this.key + "=" + this.value;
    }
    
    @JsonIgnore
    public boolean isValueInt() {
        try {
            Integer.parseInt(this.value);
        } catch (NumberFormatException e) {
            return false;
        } catch (NullPointerException e) {
            return false;
        }
        
        return true;
    }
}
