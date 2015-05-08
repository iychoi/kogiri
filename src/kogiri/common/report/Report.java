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
package kogiri.common.report;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import kogiri.common.helpers.TimeHelper;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author iychoi
 */
public class Report {
    private List<Job> jobs;
    
    public Report() {
        this.jobs = new ArrayList<Job>();
    }

    public void addJob(Job job) {
        this.jobs.add(job);
    }
    
    public void addJob(Job[] jobs) {
        for(Job job : jobs) {
            this.jobs.add(job);
        }
    }
    
    public void addJob(List<Job> jobs) {
        this.jobs.addAll(jobs);
    }
    
    public void writeTo(String filename) throws IOException {
        writeTo(new File("./", filename));
    }
    
    public void writeTo(File f) throws IOException {
        if(f.getParentFile() != null) {
            if(!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
        }
        
        Writer writer = new FileWriter(f);
        boolean first = true;
        for(Job job : this.jobs) {
            if(first) {
                first = false;
            }
            writer.write(makeText(job));
            writer.write("\n\n");
        }
        
        writer.close();
    }
    
    private String makeText(Job job) {
        String jobName = job.getJobName();
        String jobID = job.getJobID().toString();
        String jobStatus;
        try {
            jobStatus = job.getJobState().name();
        } catch (IOException ex) {
            jobStatus = "Unknown";
        } catch (InterruptedException ex) {
            jobStatus = "Unknown";
        }
        
        String startTimeStr;
        try {
            startTimeStr = TimeHelper.getTimeString(job.getStartTime());
        } catch (Exception ex) {
            startTimeStr = "Unknown";
        }
        
        String finishTimeStr;
        try {
            finishTimeStr = TimeHelper.getTimeString(job.getFinishTime());
        } catch (Exception ex) {
            finishTimeStr = "Unknown";
        }
        
        String timeTakenStr;
        try {
            timeTakenStr = TimeHelper.getDiffTimeString(job.getStartTime(), job.getFinishTime());
        } catch (Exception ex) {
            timeTakenStr = "Unknown";
        }
        
        String countersStr;
        try {
            countersStr = job.getCounters().toString();
        } catch (Exception ex) {
            countersStr = "Unknown";
        }
        
        return "Job : " + jobName + "\n" +
                "JobID : " + jobID + "\n" + 
                "Status : " + jobStatus + "\n" +
                "StartTime : " + startTimeStr + "\n" +
                "FinishTime : " + finishTimeStr + "\n" + 
                "TimeTaken : " + timeTakenStr + "\n\n" +
                countersStr;
    }
}
