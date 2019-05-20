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

import java.util.Collection;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.TableDesc;

/**
 * Any ISource that wishes to serve as input of MapReduce build engine must adapt to this interface.
 *
 * 这是BatchCubingJobBuilder2对数据源的要求，所有希望接入MRBatchCubingEngine2的数据源都必须实现该接口。
 */
public interface IMRInput {

    /** Return a helper to participate in batch cubing job flow.
     *
     * 方法返回一个IMRBatchCubingInputSide对象，参与创建一个CubeSegment的构建工作流，它内部包含三个方法，
     * addStepPhase1_CreateFlatTable()方法由构建引擎调用，要求数据源在工作流中添加步骤完成平表的创建；
     * getFlatTableInputFormat()方法帮助MR任务读取之前创建的平表；addStepPhase4_Cleanup()是进行清理收尾，
     * 清除已经没用的平表和其它临时对象，这三个方法将由构建引擎依次调用。
     * */
    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc);

    /** Return an InputFormat that reads from specified table.
     *
     * 方法返回一个IMRTableInputFormat对象，用以帮助MR任务从数据源中读取指定的关系表，为了适应MR编程接口，其中又有两个方法，
     * configureJob在启动MR任务前被调用，负责配置所需的InputFormat，连接数据源中的关系表。由于不同的InputFormat所读入的对象类型
     * 各不相同，为了使得构建引擎能够统一处理，因此又引入了parseMapperInput方法，对Mapper的每一行输入都会调用该方法一次，
     * 该方法的输入是Mapper的输入，具体类型取决于InputFormat，输出为统一的字符串数组，每列为一个元素。整体表示关系表中的一行。
     * 这样Mapper救能遍历数据源中的表了。
     * */
    public IMRTableInputFormat getTableInputFormat(TableDesc table, String uuid);

    /** Return a helper to participate in batch cubing merge job flow. */
    public IMRBatchMergeInputSide getBatchMergeInputSide(ISegment seg);

    /**
     * Utility that configures mapper to read from a table.
     *
     * IMRTableInputFormat是一个辅助接口，用来帮助Mapper读取数据源中的一张表
     */
    public interface IMRTableInputFormat {

        /** Configure the InputFormat of given job.
         * 配置给指定MapReduce任务的InputFormat
         * */
        public void configureJob(Job job);

        /** Parse a mapper input object into column values. */
        public Collection<String[]> parseMapperInput(Object mapperInput);

        /** Get the signature for the input split
         *
         * 解析Mapper的输入对象，返回关系表中的一行
         * */
        public String getInputSplitSignature(InputSplit inputSplit);
    }

    /**
     * Participate the batch cubing flow as the input side. Responsible for creating
     * intermediate flat table (Phase 1) and clean up any leftover (Phase 4).
     * 
     * - Phase 1: Create Flat Table
     * - Phase 2: Build Dictionary (with FlatTableInputFormat)
     * - Phase 3: Build Cube (with FlatTableInputFormat)
     * - Phase 4: Update Metadata & Cleanup
     *
     * IMRBatchCubingInputSide内部包含三个方法，
     * addStepPhase1_CreateFlatTable()方法由构建引擎调用，要求数据源在工作流中添加步骤完成平表的创建；
     * getFlatTableInputFormat()方法帮助MR任务读取之前创建的平表；addStepPhase4_Cleanup()是进行清理收尾，
     * 清除已经没用的平表和其它临时对象，这三个方法将由构建引擎依次调用。
     */
    public interface IMRBatchCubingInputSide {

        /** Return an InputFormat that reads from the intermediate flat table
         * 帮助MR任务读取之前创建的评表
         * */
        public IMRTableInputFormat getFlatTableInputFormat();

        /** Add step that creates an intermediate flat table as defined by CubeJoinedFlatTableDesc
         *
         * 由构建引擎调用，要求数据源在工作流中添加步骤完成平表的创建
         * */
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow);

        /** Add step that does necessary clean up, like delete the intermediate flat table
         *
         * 清理首尾，清除已经没有的平表和其他临时对象
         * */
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow);
    }

    public interface IMRBatchMergeInputSide {

        /** Add step that executes before merge dictionary and before merge cube. */
        public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow);

    }
}
