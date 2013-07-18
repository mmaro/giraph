/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

public class Hadoop23Shims extends HadoopShimsSecure {

	@Override
	public TaskAttemptContext createTaskAttemptContext(Configuration conf,
			TaskAttemptID taskId) {
		return new TaskAttemptContextImpl(conf instanceof JobConf? new JobConf(conf) : conf, taskId);
	}
	

	@Override
	public JobContext createJobContext(Configuration conf, JobID jobId) {
		return new JobContextImpl(conf, jobId);
	}
	

	@Override
	public TaskID createTaskID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TaskAttemptID createTaskAttemptID() {
		// TODO Auto-generated method stub
		return null;
	}

}
