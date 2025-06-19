CREATE TABLE result_tb (
vid bigint,
circle_length bigint
) WITH (
type='file',
geaflow.dsl.file.path='${target}'
);
-- 使用modern图
USE GRAPH modern;
-- 调用算法并插入结果
INSERT INTO result_tb
CALL single_vertex_circles_detection() YIELD (vertex_id, circle_length)
RETURN vertex_id, circle_length;