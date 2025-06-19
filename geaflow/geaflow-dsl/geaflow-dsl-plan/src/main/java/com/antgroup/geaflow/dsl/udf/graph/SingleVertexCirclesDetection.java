/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.type.primitive.BooleanType;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import com.antgroup.geaflow.common.tuple.Tuple;

@Description(name = "single_vertex_circles_detection", description = "built-in udf for SingleVertexCirclesDetection, detect self-loop/circle for each vertex by message passing")
public class SingleVertexCirclesDetection implements AlgorithmUserFunction<Long, Tuple<Long, Integer>> {
    private AlgorithmRuntimeContext<Long, Tuple<Long, Integer>> context;
    private static final int MAX_DEPTH = 10; // 可根据需要调整最大深度
    private Set<Long> verticesInCircle = new HashSet<>();
    private Set<Tuple<Long, Integer>> circleResults = new HashSet<>();

    @Override
    public void init(AlgorithmRuntimeContext<Long, Tuple<Long, Integer>> context, Object[] params) {
        this.context = context;
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Long, Integer>> messages) {
        Long selfId = (Long) TypeCastUtil.cast(vertex.getId(), Long.class);
        long iteration = context.getCurrentIterationId();
        if (iteration == 1L) {
            // 第一轮：每个顶点向所有出边邻居发送Tuple(起点ID, 路径长度=1)
            Tuple<Long, Integer> msg = Tuple.of(selfId, 1);
            for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                Long targetId = (Long) TypeCastUtil.cast(edge.getTargetId(), Long.class);
                context.sendMessage(targetId, msg);
            }
        } else {
            // 后续轮次：收到消息，判断是否成环，否则继续转发
            while (messages.hasNext()) {
                Tuple<Long, Integer> msg = messages.next();
                Long startId = msg.getF0();
                int pathLen = msg.getF1();
                if (Objects.equals(selfId, startId)) {
                    // 检测到环路，记录该顶点ID和环的长度
                    verticesInCircle.add(selfId);
                    circleResults.add(Tuple.of(selfId, pathLen));
                    continue;
                }
                if (pathLen >= MAX_DEPTH) continue;
                Tuple<Long, Integer> newMsg = Tuple.of(startId, pathLen + 1);
                for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                    Long targetId = (Long) TypeCastUtil.cast(edge.getTargetId(), Long.class);
                    context.sendMessage(targetId, newMsg);
                }
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        Long vertexId = (Long) TypeCastUtil.cast(vertex.getId(), Long.class);
        // 只输出在环中的顶点
        if (verticesInCircle.contains(vertexId)) {
            // 找到该顶点参与的最小环
            int minCircleLength = Integer.MAX_VALUE;
            for (Tuple<Long, Integer> result : circleResults) {
                if (Objects.equals(result.getF0(), vertexId)) {
                    minCircleLength = Math.min(minCircleLength, result.getF1());
                }
            }
            if (minCircleLength != Integer.MAX_VALUE) {
                context.take(ObjectRow.create(vertexId, minCircleLength));
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vertex_id", graphSchema.getIdType(), false),
            new TableField("circle_length", LongType.INSTANCE, false)
        );
    }
}