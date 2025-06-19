package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.tuple.Tuple;
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
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.Iterator;
import java.util.Optional;

@Description(name = "single_vertex_circles_detection", description = "built-in udf for SingleVertexCirclesDetection, detect self-loop/circle for each vertex by message passing")
public class SingleVertexCirclesDetection implements AlgorithmUserFunction<Object, Tuple<Object, Integer>> {
    private AlgorithmRuntimeContext<Object, Tuple<Object, Integer>> context;
    private static final int MAX_DEPTH = 10; // 可根据需要调整最大深度

    @Override
    public void init(AlgorithmRuntimeContext<Object, Tuple<Object, Integer>> context, Object[] params) {
        this.context = context;
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Object, Integer>> messages) {
        Object selfId = vertex.getId();
        long iteration = context.getCurrentIterationId();
        if (iteration == 1L) {
            // 第一轮：每个顶点向所有出边邻居发送Tuple(起点ID, 路径长度=1)
            Tuple<Object, Integer> msg = Tuple.of(selfId, 1);
            for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                context.sendMessage(edge.getTargetId(), msg);
            }
        } else {
            // 后续轮次：收到消息，判断是否成环，否则继续转发
            while (messages.hasNext()) {
                Tuple<Object, Integer> msg = messages.next();
                Object startId = msg.getF0();
                int pathLen = msg.getF1();
                if (selfId.equals(startId)) {
                    // 检测到环路，输出该顶点ID
                    System.out.println("图中存在环路，顶点ID: " + selfId);
                    context.take(ObjectRow.create(selfId));
                    continue;
                }
                if (pathLen >= MAX_DEPTH) {
                    continue;
                }
                Tuple<Object, Integer> newMsg = Tuple.of(startId, pathLen + 1);
                for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                    context.sendMessage(edge.getTargetId(), newMsg);
                }
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        // 这里可以根据需要输出最终结果，目前留空即可
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false)
        );
    }
} 