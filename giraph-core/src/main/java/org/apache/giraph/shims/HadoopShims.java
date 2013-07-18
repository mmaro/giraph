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
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

/**
 * Shim layer to abstract differences between Hadoop versions.
 */
public interface HadoopShims {
	
	public static abstract class Instance {
		static HadoopShims instance = selectShim();
		
		public static HadoopShims get() {
            return instance;
        }
		
		private static HadoopShims selectShim() {
			String major = ShimLoader.getMajorVersion();
			String shimFQN = "org.apache.giraph.shims.Hadoop20SShims";
			if (major.startsWith("0.23")) {
				shimFQN = "org.apache.giraph.shims.Hadoop23Shims";
			}
			try {
                Class<? extends HadoopShims> clasz = Class.forName(shimFQN)
                    .asSubclass(HadoopShims.class);
                return clasz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate: " + shimFQN, e);
            }
		}
		
	}

  /**
   * Create Task ID
   * @return Task Id
   */
  TaskID createTaskID();

  /**
   * Create Task Attempt ID
   * @return Task Attempt Id
   */
  TaskAttemptID createTaskAttemptID();

  public org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext(Configuration conf,
                                                                                 TaskAttemptID taskId);

  public JobContext createJobContext(Configuration conf, JobID jobId);

}
