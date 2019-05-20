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

package org.apache.kylin.engine.mr.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsDecisionUtil {
    protected static final Logger logger = LoggerFactory.getLogger(StatisticsDecisionUtil.class);

    public static void decideCubingAlgorithm(CubingJob cubingJob, CubeSegment seg) throws IOException {
        CubeStatsReader cubeStats = new CubeStatsReader(seg, null, seg.getConfig());
        decideCubingAlgorithm(cubingJob, seg, cubeStats.getMapperOverlapRatioOfFirstBuild(),
                cubeStats.getMapperNumberOfFirstBuild());
    }

    /**
     * 用户该如何选择算法呢?无需担心，Kylin会自动选择合适的算法。Kylin在计算Cube之前对数据进行采样，在“fact distinct”步，
     * 利用HyperLogLog模拟去重，估算每种组合有多少不同的key，从而计算出每个Mapper输出的数据大小，以及所有Mapper之间数据的重合度，
     * 据此来决定采用哪种算法更优。在对上百个Cube任务的时间做统计分析后，Kylin选择了7做为默认的算法选择阀值(
     * 参数kylin.cube.algorithm.layer-or-inmem-threshold)：如果各个Mapper的小Cube的行数之和，大于reduce后的Cube行数的7倍，
     * 采用Layered Cubing, 反之采用Fast Cubing。如果用户在使用过程中，更倾向于使用Fast Cubing，可以适当调大此参数值，反之调小。
     *
     * @param cubingJob
     * @param seg
     * @param mapperOverlapRatio
     * @param mapperNumber
     * @throws IOException
     */
    public static void decideCubingAlgorithm(CubingJob cubingJob, CubeSegment seg, double mapperOverlapRatio,
            int mapperNumber) throws IOException {
        KylinConfig kylinConf = seg.getConfig();
        String algPref = kylinConf.getCubeAlgorithm();
        CubingJob.AlgorithmEnum alg;
        if (mapperOverlapRatio == 0) { // no source records
            alg = CubingJob.AlgorithmEnum.INMEM;
        } else if (CubingJob.AlgorithmEnum.INMEM.name().equalsIgnoreCase(algPref)) {
            alg = CubingJob.AlgorithmEnum.INMEM;
        } else if (CubingJob.AlgorithmEnum.LAYER.name().equalsIgnoreCase(algPref)) {
            alg = CubingJob.AlgorithmEnum.LAYER;
        } else {
            int memoryHungryMeasures = 0;
            for (MeasureDesc measure : seg.getCubeDesc().getMeasures()) {
                if (measure.getFunction().getMeasureType().isMemoryHungry()) {
                    logger.info("This cube has memory-hungry measure " + measure.getFunction().getExpression());
                    memoryHungryMeasures++;
                }
            }

            if (memoryHungryMeasures > 0) {
                alg = CubingJob.AlgorithmEnum.LAYER;
            } else if ("random".equalsIgnoreCase(algPref)) { // for testing
                alg = new Random().nextBoolean() ? CubingJob.AlgorithmEnum.INMEM : CubingJob.AlgorithmEnum.LAYER;
            } else { // the default
                int mapperNumLimit = kylinConf.getCubeAlgorithmAutoMapperLimit();
                double overlapThreshold = kylinConf.getCubeAlgorithmAutoThreshold();
                logger.info("mapperNumber for " + seg + " is " + mapperNumber + " and threshold is " + mapperNumLimit);
                logger.info("mapperOverlapRatio for " + seg + " is " + mapperOverlapRatio + " and threshold is "
                        + overlapThreshold);

                // in-mem cubing is good when
                // 1) the cluster has enough mapper slots to run in parallel
                // 2) the mapper overlap ratio is small, meaning the shuffle of in-mem MR has advantage
                alg = (mapperNumber <= mapperNumLimit && mapperOverlapRatio <= overlapThreshold)//
                        ? CubingJob.AlgorithmEnum.INMEM  // 快速算法
                        : CubingJob.AlgorithmEnum.LAYER; // 逐层算法
            }

        }
        logger.info("The cube algorithm for " + seg + " is " + alg);

        cubingJob.setAlgorithm(alg);
    }

    // For triggering cube planner phase one
    public static void optimizeCubingPlan(CubeSegment segment) throws IOException {
        if (isAbleToOptimizeCubingPlan(segment)) {
            logger.info("It's able to trigger cuboid planner algorithm.");
        } else {
            return;
        }

        Map<Long, Long> recommendCuboidsWithStats = CuboidRecommenderUtil.getRecommendCuboidList(segment);
        if (recommendCuboidsWithStats == null || recommendCuboidsWithStats.isEmpty()) {
            return;
        }

        CubeInstance cube = segment.getCubeInstance();
        CubeUpdate update = new CubeUpdate(cube.latestCopyForWrite());
        update.setCuboids(recommendCuboidsWithStats);
        CubeManager.getInstance(cube.getConfig()).updateCube(update);
    }

    public static boolean isAbleToOptimizeCubingPlan(CubeSegment segment) {
        CubeInstance cube = segment.getCubeInstance();
        if (!cube.getConfig().isCubePlannerEnabled())
            return false;

        if (cube.getSegments(SegmentStatusEnum.READY_PENDING).size() > 0) {
            logger.info("Has read pending segments and will not enable cube planner.");
            return false;
        }
        List<CubeSegment> readySegments = cube.getSegments(SegmentStatusEnum.READY);
        List<CubeSegment> newSegments = cube.getSegments(SegmentStatusEnum.NEW);
        if (newSegments.size() <= 1 && //
                (readySegments.size() == 0 || //
                        (cube.getConfig().isCubePlannerEnabledForExistingCube() && readySegments.size() == 1
                                && readySegments.get(0).getSegRange().equals(segment.getSegRange())))) {
            return true;
        } else {
            return false;
        }
    }
}
