package org.apache.giraph.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public abstract class HadoopShimsSecure implements HadoopShims {
	
	
	  @Override
	  abstract public org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext(Configuration conf,
              TaskAttemptID taskId);

	  @Override
	  abstract public org.apache.hadoop.mapreduce.JobContext createJobContext(Configuration conf, JobID jobId);

}
