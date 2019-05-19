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

import java.util.List;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidUtil;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMROutput2.IMRBatchCubingOutputSide2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.BaseCuboidJob;
import org.apache.kylin.engine.mr.steps.InMemCuboidJob;
import org.apache.kylin.engine.mr.steps.NDCuboidJob;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchCubingJobBuilder2 extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(BatchCubingJobBuilder2.class);

    private final IMRBatchCubingInputSide inputSide;
    private final IMRBatchCubingOutputSide2 outputSide;

    public BatchCubingJobBuilder2(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
        /** 创建CUBE数据的输入端，目前支持 hive  jdbc kafak */
        this.inputSide = MRUtil.getBatchCubingInputSide(seg);
        /** 创建CUBE数据的输出端，目前支持 DruidStorage HBASE  HybridStorage  */
        this.outputSide = MRUtil.getBatchCubingOutputSide2(seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to BUILD segment " + seg);

        // 获得一个初始化的 Job 实例     //构建job任务（DefaultChainedExecutable类型，是一个任务链）
        final CubingJob result = CubingJob.createBuildJob(seg, submitter, config);
        final String jobId = result.getId();
        // 获取 cuboid 的数据路径，以配置的 working-dir 开头 ，配置文件中配置 kylin.env.hdfs-working-dir 默认 /kylin
        final String cuboidRootPath = getCuboidRootPath(jobId);

        // Phase 1: Create Flat Table & Materialize Hive View in Lookup Tables
        inputSide.addStepPhase1_CreateFlatTable(result);

        // Phase 2: Build Dictionary
        result.addTask(createFactDistinctColumnsStep(jobId));

        // 判断是否是高基维（UHC），如果是则添加新的任务对高基维进行处理
        if (isEnableUHCDictStep()) {
            result.addTask(createBuildUHCDictStep(jobId));
        }

        // 构建字典
        result.addTask(createBuildDictionaryStep(jobId));
        // 保存 cuboid 统计数据
        result.addTask(createSaveStatisticsStep(jobId));

        // add materialize lookup tables if needed
        LookupMaterializeContext lookupMaterializeContext = addMaterializeLookupTableSteps(result);

        // 创建 HTable
        outputSide.addStepPhase2_BuildDictionary(result);

        if (seg.getCubeDesc().isShrunkenDictFromGlobalEnabled()) {
            result.addTask(createExtractDictionaryFromGlobalJob(jobId));
        }

        // Phase 3: Build Cube
        // 构建CUBE
        addLayerCubingSteps(result, jobId, cuboidRootPath); // layer cubing, only selected algorithm will execute
        addInMemCubingSteps(result, jobId, cuboidRootPath); // inmem cubing, only selected algorithm will execute
        outputSide.addStepPhase3_BuildCube(result);

        // Phase 4: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterBuildStep(jobId, lookupMaterializeContext));
        inputSide.addStepPhase4_Cleanup(result);
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    public boolean isEnableUHCDictStep() {
        if (!config.getConfig().isBuildUHCDictWithMREnabled()) {
            return false;
        }

        List<TblColRef> uhcColumns = seg.getCubeDesc().getAllUHCColumns();
        if (uhcColumns.size() == 0) {
            return false;
        }

        return true;
    }

    protected void addLayerCubingSteps(final CubingJob result, final String jobId, final String cuboidRootPath) {
        // Don't know statistics so that tree cuboid scheduler is not determined. Determine the maxLevel at runtime
        // 不知道统计数据，因此无法确定树长方体调度程序。在运行时确定maxLevel
        final int maxLevel = CuboidUtil.getLongestDepth(seg.getCuboidScheduler().getAllCuboidIds());
        // base cuboid step
        result.addTask(createBaseCuboidStep(getCuboidOutputPathsByLevel(cuboidRootPath, 0), jobId));
        // n dim cuboid steps
        for (int i = 1; i <= maxLevel; i++) {
            result.addTask(createNDimensionCuboidStep(getCuboidOutputPathsByLevel(cuboidRootPath, i - 1), getCuboidOutputPathsByLevel(cuboidRootPath, i), i, jobId));
        }
    }

    protected void addInMemCubingSteps(final CubingJob result, String jobId, String cuboidRootPath) {
        // base cuboid job
        MapReduceExecutable cubeStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        cubeStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidRootPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Cube_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        if (seg.getCubeDesc().isShrunkenDictFromGlobalEnabled()) {
            appendExecCmdParameters(cmd, BatchConstants.ARG_SHRUNKEN_DICT_PATH, getShrunkenDictionaryPath(jobId));
        }

        cubeStep.setMapReduceParams(cmd.toString());
        cubeStep.setMapReduceJobClass(getInMemCuboidJob());
        result.addTask(cubeStep);
    }

    protected Class<? extends AbstractHadoopJob> getInMemCuboidJob() {
        return InMemCuboidJob.class;
    }

    private MapReduceExecutable createBaseCuboidStep(String cuboidOutputPath, String jobId) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, "FLAT_TABLE"); // marks flat table input
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidOutputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Base_Cuboid_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "0");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        if (seg.getCubeDesc().isShrunkenDictFromGlobalEnabled()) {
            appendExecCmdParameters(cmd, BatchConstants.ARG_SHRUNKEN_DICT_PATH, getShrunkenDictionaryPath(jobId));
        }

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(getBaseCuboidJob());
        //        baseCuboidStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return baseCuboidStep;
    }

    protected Class<? extends AbstractHadoopJob> getBaseCuboidJob() {
        return BaseCuboidJob.class;
    }

    private MapReduceExecutable createNDimensionCuboidStep(String parentPath, String outputPath, int level, String jobId) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : level " + level);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, parentPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_ND-Cuboid_Builder_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "" + level);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(getNDCuboidJob());
        return ndCuboidStep;
    }

    protected Class<? extends AbstractHadoopJob> getNDCuboidJob() {
        return NDCuboidJob.class;
    }
}
