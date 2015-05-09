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
package kogiri.common.algorithms;

import kogiri.common.helpers.SequenceHelper;

/**
 *
 * @author iychoi
 */
public class KmerKeySelection {
    public KmerKeySelection() {
        
    }
    
    public static enum KmerForm {
        FORWARD,
        REVERSE_COMPLEMENT,
    }
    
    public String selectKey(String kmer) {
        String rkmer = SequenceHelper.getReverseComplement(kmer);
            
        String kmerKey = kmer;
        if(rkmer.compareTo(kmer) < 0) {
            kmerKey = rkmer;
        }

        return kmerKey;
    }
    
    public static class KmerRecord {

        private String sequence;
        private KmerForm form;

        public KmerRecord(String sequence) {
            this.sequence = sequence;
            this.form = KmerForm.FORWARD;
        }
        
        public KmerRecord(String sequence, KmerForm form) {
            this.sequence = sequence;
            this.form = form;
        }

        public String getSequence() {
            return this.sequence;
        }

        public KmerForm getForm() {
            return this.form;
        }

        public boolean isForward() {
            return this.form == KmerForm.FORWARD;
        }

        public boolean isReverseComplement() {
            return this.form == KmerForm.REVERSE_COMPLEMENT;
        }

        public KmerRecord getReverseComplement() {
            if (this.form == KmerForm.FORWARD) {
                return new KmerRecord(SequenceHelper.getReverseComplement(this.sequence), KmerForm.REVERSE_COMPLEMENT);
            } else {
                return new KmerRecord(SequenceHelper.getReverseComplement(this.sequence), KmerForm.FORWARD);
            }
        }
        
        public KmerRecord getOriginalForm() {
            if (this.form == KmerForm.FORWARD) {
                return this;
            } else {
                return getReverseComplement();
            }
        }

        public KmerRecord getSelectedKey() {
            String rcSeq = SequenceHelper.getReverseComplement(this.sequence);
            if (rcSeq.compareTo(this.sequence) < 0) {
                if (this.form == KmerForm.FORWARD) {
                    return new KmerRecord(rcSeq, KmerForm.REVERSE_COMPLEMENT);
                } else {
                    return new KmerRecord(rcSeq, KmerForm.FORWARD);
                }
            } else {
                return this;
            }
        }
    }

}
