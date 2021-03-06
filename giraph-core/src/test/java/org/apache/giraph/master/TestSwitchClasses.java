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

package org.apache.giraph.master;

import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import junit.framework.Assert;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/** Test switching Computation and Combiner class during application */
public class TestSwitchClasses {
  @Test
  public void testSwitchingClasses() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(Computation3.class);
    conf.setMasterComputeClass(SwitchingClassesMasterCompute.class);

    TestGraph<IntWritable, StatusValue, IntWritable> graph =
        new TestGraph<IntWritable, StatusValue, IntWritable>(conf);
    IntWritable id1 = new IntWritable(1);
    graph.addVertex(id1, new StatusValue());
    IntWritable id2 = new IntWritable(2);
    graph.addVertex(id2, new StatusValue());
    graph = InternalVertexRunner.run(conf, graph);

    Assert.assertEquals(2, graph.getVertices().size());
    StatusValue value1 = graph.getVertex(id1).getValue();
    StatusValue value2 = graph.getVertex(id2).getValue();

    // Check that computations were performed in expected order
    ArrayList<Integer> expectedComputations = Lists.newArrayList(1, 1, 2, 3, 1);
    checkComputations(expectedComputations, value1.computations);
    checkComputations(expectedComputations, value2.computations);

    // Check that messages were sent in the correct superstep,
    // and combined when needed
    ArrayList<HashSet<Double>> messages1 =
        Lists.newArrayList(
            Sets.<Double>newHashSet(),
            Sets.<Double>newHashSet(11d),
            Sets.<Double>newHashSet(11d),
            Sets.<Double>newHashSet(101.5, 201.5),
            Sets.<Double>newHashSet(3002d));
    checkMessages(messages1, value1.messagesReceived);
    ArrayList<HashSet<Double>> messages2 =
        Lists.newArrayList(
            Sets.<Double>newHashSet(),
            Sets.<Double>newHashSet(12d),
            Sets.<Double>newHashSet(12d),
            Sets.<Double>newHashSet(102.5, 202.5),
            Sets.<Double>newHashSet(3004d));
    checkMessages(messages2, value2.messagesReceived);
  }

  private static void checkComputations(ArrayList<Integer> expected,
      ArrayList<Integer> actual) {
    Assert.assertEquals("Incorrect number of supersteps",
        expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals("Incorrect computation on superstep " + i,
          (int) expected.get(i), (int) actual.get(i));
    }
  }

  private static void checkMessages(ArrayList<HashSet<Double>> expected,
      ArrayList<HashSet<Double>> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(expected.get(i).size(), actual.get(i).size());
      for (Double value : expected.get(i)) {
        Assert.assertTrue(actual.get(i).contains(value));
      }
    }
  }

  public static class SwitchingClassesMasterCompute
      extends DefaultMasterCompute {
    @Override
    public void compute() {
      switch ((int) getSuperstep()) {
        case 0:
          setComputation(Computation1.class);
          setCombiner(MinimumCombiner.class);
          break;
        case 1:
          // test classes don't change
          break;
        case 2:
          setComputation(Computation2.class);
          // test combiner removed
          setCombiner(null);
          break;
        case 3:
          setComputation(Computation3.class);
          setCombiner(SumCombiner.class);
          break;
        case 4:
          setComputation(Computation1.class);
          break;
        default:
          haltComputation();
      }
    }
  }

  public static class Computation1 extends Computation<IntWritable,
      StatusValue, IntWritable, IntWritable, IntWritable> {
    @Override
    public void compute(Vertex<IntWritable, StatusValue, IntWritable> vertex,
        Iterable<IntWritable> messages) throws IOException {
      vertex.getValue().computations.add(1);
      vertex.getValue().addIntMessages(messages);

      IntWritable otherId = new IntWritable(3 - vertex.getId().get());
      sendMessage(otherId, new IntWritable(otherId.get() + 10));
      sendMessage(otherId, new IntWritable(otherId.get() + 20));
    }
  }

  public static class Computation2 extends Computation<IntWritable,
      StatusValue, IntWritable, IntWritable, DoubleWritable> {
    @Override
    public void compute(Vertex<IntWritable, StatusValue, IntWritable> vertex,
        Iterable<IntWritable> messages) throws IOException {
      vertex.getValue().computations.add(2);
      vertex.getValue().addIntMessages(messages);

      IntWritable otherId = new IntWritable(3 - vertex.getId().get());
      sendMessage(otherId, new DoubleWritable(otherId.get() + 100.5));
      sendMessage(otherId, new DoubleWritable(otherId.get() + 200.5));
    }
  }

  public static class Computation3 extends Computation<IntWritable,
      StatusValue, IntWritable, DoubleWritable, IntWritable> {
    @Override
    public void compute(
        Vertex<IntWritable, StatusValue, IntWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {
      vertex.getValue().computations.add(3);
      vertex.getValue().addDoubleMessages(messages);

      IntWritable otherId = new IntWritable(3 - vertex.getId().get());
      sendMessage(otherId, new IntWritable(otherId.get() + 1000));
      sendMessage(otherId, new IntWritable(otherId.get() + 2000));
    }
  }

  public static class MinimumCombiner extends Combiner<IntWritable,
      IntWritable> {
    @Override
    public void combine(IntWritable vertexIndex, IntWritable originalMessage,
        IntWritable messageToCombine) {
      originalMessage.set(
          Math.min(originalMessage.get(), messageToCombine.get()));
    }

    @Override
    public IntWritable createInitialMessage() {
      return new IntWritable(Integer.MAX_VALUE);
    }
  }

  public static class SumCombiner extends Combiner<IntWritable, IntWritable> {
    @Override
    public void combine(IntWritable vertexIndex, IntWritable originalMessage,
        IntWritable messageToCombine) {
      originalMessage.set(originalMessage.get() + messageToCombine.get());
    }

    @Override
    public IntWritable createInitialMessage() {
      return new IntWritable(0);
    }
  }

  public static class StatusValue implements Writable {
    private ArrayList<Integer> computations = new ArrayList<Integer>();
    private ArrayList<HashSet<Double>> messagesReceived =
        new ArrayList<HashSet<Double>>();

    public StatusValue() {
    }

    public void addIntMessages(Iterable<IntWritable> messages) {
      HashSet<Double> messagesList = new HashSet<Double>();
      for (IntWritable message : messages) {
        messagesList.add((double) message.get());
      }
      messagesReceived.add(messagesList);
    }

    public void addDoubleMessages(Iterable<DoubleWritable> messages) {
      HashSet<Double> messagesList = new HashSet<Double>();
      for (DoubleWritable message : messages) {
        messagesList.add(message.get());
      }
      messagesReceived.add(messagesList);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeInt(computations.size());
      for (Integer computation : computations) {
        dataOutput.writeInt(computation);
      }
      dataOutput.writeInt(messagesReceived.size());
      for (HashSet<Double> messages : messagesReceived) {
        dataOutput.writeInt(messages.size());
        for (Double message : messages) {
          dataOutput.writeDouble(message);
        }
      }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      int size = dataInput.readInt();
      computations = new ArrayList<Integer>(size);
      for (int i = 0; i < size; i++) {
        computations.add(dataInput.readInt());
      }
      size = dataInput.readInt();
      messagesReceived = new ArrayList<HashSet<Double>>(size);
      for (int i = 0; i < size; i++) {
        int size2 = dataInput.readInt();
        HashSet<Double> messages = new HashSet<Double>(size2);
        for (int j = 0; j < size2; j++) {
          messages.add(dataInput.readDouble());
        }
        messagesReceived.add(messages);
      }
    }
  }
}
