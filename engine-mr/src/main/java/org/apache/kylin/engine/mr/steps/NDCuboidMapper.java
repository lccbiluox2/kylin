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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CuboidSchedulerUtil;
import org.apache.kylin.engine.mr.common.NDCuboidBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author George Song (ysong1)
 * 
 */
public class NDCuboidMapper extends KylinMapper<Text, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(NDCuboidMapper.class);

    private Text outputKey = new Text();
    private String cubeName;
    private String segmentID;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;
    private CuboidScheduler cuboidScheduler;

    private int handleCounter;
    private int skipCounter;

    private RowKeySplitter rowKeySplitter;

    private NDCuboidBuilder ndCuboidBuilder;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        String cuboidModeName = context.getConfiguration().get(BatchConstants.CFG_CUBOID_MODE);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegmentById(segmentID);
        ndCuboidBuilder = new NDCuboidBuilder(cubeSegment);
        // initialize CubiodScheduler
        cuboidScheduler = CuboidSchedulerUtil.getCuboidSchedulerByMode(cubeSegment, cuboidModeName);
        rowKeySplitter = new RowKeySplitter(cubeSegment);
    }



    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        long cuboidId = rowKeySplitter.split(key.getBytes());
        Cuboid parentCuboid = Cuboid.findForMandatory(cubeDesc, cuboidId);

        /**
         * Build N-Dimension Cuboid
           ## 构建N维cuboid
           这些步骤是“逐层”构建cube的过程，每一步以前一步的输出作为输入，然后去掉一个维度以聚合得到一个子cuboid。举个例子，cuboid ABCD去掉A得到BCD，去掉B得到ACD。
           有些cuboid可以从一个以上的父cuboid聚合得到，这种情况下，Kylin会选择最小的一个父cuboid。举例,AB可以从ABC(id:1110)和ABD(id:1101)生成，则ABD会被选中，因为它的比ABC要小。
           在这基础上，如果D的基数较小，聚合运算的成本就会比较低。所以，当设计rowkey序列的时候，请记得将基数较小的维度放在末尾。这样不仅有利于cube构建，而且有助于cube查询，因为预聚合也遵循相同的规则。
           通常来说，从N维到(N/2)维的构建比较慢，因为这是cuboid数量爆炸性增长的阶段：N维有1个cuboid，(N-1)维有N个cuboid，(N-2)维有(N-2)*(N-1)个cuboid，以此类推。经过(N/2)维构建的步骤，整个构建任务会逐渐变快。
        */
        Collection<Long> myChildren = cuboidScheduler.getSpanningCuboid(cuboidId);

        // if still empty or null
        if (myChildren == null || myChildren.size() == 0) {
            context.getCounter(BatchConstants.MAPREDUCE_COUNTER_GROUP_NAME, "Skipped records").increment(1L);
            if (skipCounter++ % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
                logger.info("Skipping record with ordinal: " + skipCounter);
            }
            return;
        }

        context.getCounter(BatchConstants.MAPREDUCE_COUNTER_GROUP_NAME, "Processed records").increment(1L);

        if (handleCounter++ % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handling record with ordinal: " + handleCounter);
            logger.info("Parent cuboid: " + parentCuboid.getId() + "; Children: " + myChildren);
        }

        Pair<Integer, ByteArray> result;
        for (Long child : myChildren) {
            Cuboid childCuboid = Cuboid.findForMandatory(cubeDesc, child);
            result = ndCuboidBuilder.buildKey(parentCuboid, childCuboid, rowKeySplitter.getSplitBuffers());
            outputKey.set(result.getSecond().array(), 0, result.getFirst());
            context.write(outputKey, value);
        }

    }
}
