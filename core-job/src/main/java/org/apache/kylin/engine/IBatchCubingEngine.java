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

package org.apache.kylin.engine;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;


/**
 * 每一个构建引擎必须实现接口
 *
 * 并在EngineFactory中注册实现类。只有这样才能在Cube元数据中引用该引擎，否则会在构建Cube时出现“找不到实现”的错误。
 * 注册的方法是通过kylin.properties来完成的。在其中添加一行构建引擎的声明。比如：
 *
 * kylin.engine.default=2   EngineFactory在启动时会读取kylin.properties，默认引擎即为标号2的MRBatchCubingEngine2这个引擎。
 */
public interface IBatchCubingEngine {
    
    /** Mark deprecated to indicate for test purpose only */
    @Deprecated
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeDesc cubeDesc);
    
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeSegment newSegment);

    /** Build a new cube segment, typically its time range appends to the end of current cube.
     *
     * 返回一个工作流计划，用以构建指定的CubeSegment。这里的CubeSegment是一个刚完成初始化，但还不包含数据的CubeSegment。
     * 返回的DefaultChainedExecutable是一个工作流的描述对象。它将被保存并由工作流引擎在稍后调度执行，从而完成Cube的构建。
     *
     * */
    public DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter);

    /** Merge multiple small segments into a big one.
     *
     * 返回一个工作流计划，用以合并指定的CubeSegment。这里的CubeSegment是一个待合并的CubeSegment，它的区间横跨了多个
     * 现有的CubeSegment。返回的工作流计划一样会在稍后被调度执行，执行的过程会将多个现有的CubeSegment合并为一个，
     * 从而降低Cube的碎片化成都。
     *
     * */
    public DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter);

    /** Optimize a segment based on the cuboid recommend list produced by the cube planner. */
    public DefaultChainedExecutable createBatchOptimizeJob(CubeSegment optimizeSegment, String submitter);

    /**
     * 指明该计算引擎的IN接口。
     * @return
     */
    public Class<?> getSourceInterface();

    /**
     * 指明该计算引擎的OUT接口。
     *
     * @return
     */
    public Class<?> getStorageInterface();
}
