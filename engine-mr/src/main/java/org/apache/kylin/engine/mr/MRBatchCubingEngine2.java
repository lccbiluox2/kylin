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

package org.apache.kylin.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.IBatchCubingEngine;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

public class MRBatchCubingEngine2 implements IBatchCubingEngine {

    @Override
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeDesc cubeDesc) {
        return new CubeJoinedFlatTableDesc(cubeDesc);
    }

    @Override
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeSegment newSegment) {
        return new CubeJoinedFlatTableDesc(newSegment);
    }

    /**
     *  返回一个工作流计划，泳衣构建指定的 CubeSegmens
     *
     *  DefaultChainedExecutable 代表了一种可执行的对象，其中包含了很多子任务，他的执行过程依次是串行执行每一个子任务，直到
     *  所有的子任务都执行完成，kylin的cube构建比较复杂，要执行很多步骤，步骤之间有直接的依赖性和顺序性，DefaultChainedExecutable
     *  很好的抽象出来这个连续依次执行的模型，可以用来表示cube的构建的工作了流。
     *
     * @param newSegment
     * @param submitter
     * @return
     */
    @Override
    public DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter) {
        return new BatchCubingJobBuilder2(newSegment, submitter).build();
    }

    /**
     * 返回一个工作流计划，泳衣合并指定的CubeSegment
     * @param mergeSegment
     * @param submitter
     * @return
     */
    @Override
    public DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter) {
        return new BatchMergeJobBuilder2(mergeSegment, submitter).build();
    }

    @Override
    public DefaultChainedExecutable createBatchOptimizeJob(CubeSegment optimizeSegment, String submitter) {
        return new BatchOptimizeJobBuilder2(optimizeSegment, submitter).build();
    }

    /**
     * 指明该计算引擎的IN接口。
     * @return
     */
    @Override
    public Class<?> getSourceInterface() {
        return IMRInput.class;
    }

    /**
     * 指明该计算引擎的OUT接口。
     *
     * @return
     */
    @Override
    public Class<?> getStorageInterface() {
        return IMROutput2.class;
    }

}
