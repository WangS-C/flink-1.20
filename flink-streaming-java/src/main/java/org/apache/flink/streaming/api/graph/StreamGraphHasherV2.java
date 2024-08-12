/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;

import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.StringUtils.byteToHexString;

/**
 * StreamGraphHasher from Flink 1.2. This contains duplicated code to ensure that the algorithm does
 * not change with future Flink versions.
 *
 * <p>DO NOT MODIFY THIS CLASS
 */
public class StreamGraphHasherV2 implements StreamGraphHasher {

    private static final Logger LOG = LoggerFactory.getLogger(StreamGraphHasherV2.class);

    /**
     * Returns a map with a hash for each {@link StreamNode} of the {@link StreamGraph}. The hash is
     * used as the {@link JobVertexID} in order to identify nodes across job submissions if they
     * didn't change.
     *
     * <p>The complete {@link StreamGraph} is traversed. The hash is either computed from the
     * transformation's user-specified id (see {@link Transformation#getUid()}) or generated in a
     * deterministic way.
     *
     * <p>The generated hash is deterministic with respect to:
     *
     * <ul>
     *   <li>node-local properties (node ID),
     *   <li>chained output nodes, and
     *   <li>input nodes hashes
     * </ul>
     *
     * @return A map from {@link StreamNode#id} to hash as 16-byte array.
     */
    //返回包含StreamGraph的每个流节点的哈希的映射。哈希值用作JobVertexID ，以便在作业提交中标识节点 (如果它们没有更改)。
    //遍历完整的流图。哈希可以根据转换的用户指定id (请参见transformation. getUid()) 计算，也可以以确定性方式生成。
    //生成的哈希值是确定性的:
    //节点本地属性 (节点ID)，
    //链式输出节点，以及
    //输入节点哈希
    @Override
    public Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph) {
        // The hash function used to generate the hash
        final HashFunction hashFunction = Hashing.murmur3_128(0);
        final Map<Integer, byte[]> hashes = new HashMap<>();

        Set<Integer> visited = new HashSet<>();
        Queue<StreamNode> remaining = new ArrayDeque<>();

        // We need to make the source order deterministic. The source IDs are
        // not returned in the same order, which means that submitting the same
        // program twice might result in different traversal, which breaks the
        // deterministic hash assignment.
        List<Integer> sources = new ArrayList<>();
        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            sources.add(sourceNodeId);
        }
        //我们需要使源顺序具有确定性。源id不会以相同的顺序返回，这意味着提交同一程序两次可能会导致不同的遍历，从而破坏确定性哈希分配。
        Collections.sort(sources);

        //
        // Traverse the graph in a breadth-first manner. Keep in mind that
        // the graph is not a tree and multiple paths to nodes can exist.
        //

        // Start with source nodes
        //以广度优先的方式遍历图形。请记住，图不是树，可以存在多个节点路径。从源节点开始
        for (Integer sourceNodeId : sources) {
            remaining.add(streamGraph.getStreamNode(sourceNodeId));
            visited.add(sourceNodeId);
        }

        StreamNode currentNode;
        while ((currentNode = remaining.poll()) != null) {
            // Generate the hash code. Because multiple path exist to each
            // node, we might not have all required inputs available to
            // generate the hash code.
            //生成哈希代码。由于每个节点存在多个路径，因此我们可能没有所有必需的输入可用于生成哈希代码。
            if (generateNodeHash(
                    currentNode,
                    hashFunction,
                    hashes,
                    streamGraph.isChainingEnabled(),
                    streamGraph)) {
                // Add the child nodes
                // 添加子节点
                for (StreamEdge outEdge : currentNode.getOutEdges()) {
                    StreamNode child = streamGraph.getTargetVertex(outEdge);

                    if (!visited.contains(child.getId())) {
                        remaining.add(child);
                        visited.add(child.getId());
                    }
                }
            } else {
                // We will revisit this later.
                visited.remove(currentNode.getId());
            }
        }

        return hashes;
    }

    /**
     * Generates a hash for the node and returns whether the operation was successful.
     *
     * @param node The node to generate the hash for
     * @param hashFunction The hash function to use
     * @param hashes The current state of generated hashes
     * @return <code>true</code> if the node hash has been generated. <code>false</code>, otherwise.
     *     If the operation is not successful, the hash needs be generated at a later point when all
     *     input is available.
     * @throws IllegalStateException If node has user-specified hash and is intermediate node of a
     *     chain
     */
    // 为节点生成哈希，并返回操作是否成功。
    private boolean generateNodeHash(
            StreamNode node,
            HashFunction hashFunction,
            Map<Integer, byte[]> hashes,
            boolean isChainingEnabled,
            StreamGraph streamGraph) {

        // Check for user-specified ID
        //检查用户指定的ID
        String userSpecifiedHash = node.getTransformationUID();

        if (userSpecifiedHash == null) {
            // Check that all input nodes have their hashes computed
            //检查所有输入节点的哈希值是否已计算
            for (StreamEdge inEdge : node.getInEdges()) {
                // If the input node has not been visited yet, the current
                // node will be visited again at a later point when all input
                // nodes have been visited and their hashes set.
                //如果还没有访问输入节点，则当已经访问所有输入节点并且它们的散列被设置时，将在稍后的点再次访问当前节点。
                if (!hashes.containsKey(inEdge.getSourceId())) {
                    return false;
                }
            }

            Hasher hasher = hashFunction.newHasher();
            byte[] hash =
                    generateDeterministicHash(node, hasher, hashes, isChainingEnabled, streamGraph);

            if (hashes.put(node.getId(), hash) != null) {
                // Sanity check
                throw new IllegalStateException(
                        "Unexpected state. Tried to add node hash "
                                + "twice. This is probably a bug in the JobGraph generator.");
            }

            return true;
        } else {
            Hasher hasher = hashFunction.newHasher();
            byte[] hash = generateUserSpecifiedHash(node, hasher);

            for (byte[] previousHash : hashes.values()) {
                if (Arrays.equals(previousHash, hash)) {
                    throw new IllegalArgumentException(
                            "Hash collision on user-specified ID "
                                    + "\""
                                    + userSpecifiedHash
                                    + "\". "
                                    + "Most likely cause is a non-unique ID. Please check that all IDs "
                                    + "specified via `uid(String)` are unique.");
                }
            }

            if (hashes.put(node.getId(), hash) != null) {
                // Sanity check
                throw new IllegalStateException(
                        "Unexpected state. Tried to add node hash "
                                + "twice. This is probably a bug in the JobGraph generator.");
            }

            return true;
        }
    }

    /** Generates a hash from a user-specified ID. */
    //从用户指定的ID生成哈希
    private byte[] generateUserSpecifiedHash(StreamNode node, Hasher hasher) {
        hasher.putString(node.getTransformationUID(), Charset.forName("UTF-8"));

        return hasher.hash().asBytes();
    }

    /** Generates a deterministic hash from node-local properties and input and output edges. */
    //从节点本地属性以及输入和输出边生成确定性哈希。
    private byte[] generateDeterministicHash(
            StreamNode node,
            Hasher hasher,
            Map<Integer, byte[]> hashes,
            boolean isChainingEnabled,
            StreamGraph streamGraph) {

        // Include stream node to hash. We use the current size of the computed
        // hashes as the ID. We cannot use the node's ID, because it is
        // assigned from a static counter. This will result in two identical
        // programs having different hashes.
        //包括要哈希的流节点。我们使用计算的哈希的当前大小作为ID。
        // 我们不能使用节点的ID，因为它是从静态计数器分配的。这将导致两个相同的程序具有不同的散列。
        generateNodeLocalHash(hasher, hashes.size());

        // Include chained nodes to hash
        //包括要散列的链接节点
        for (StreamEdge outEdge : node.getOutEdges()) {
            if (isChainable(outEdge, isChainingEnabled, streamGraph)) {

                // Use the hash size again, because the nodes are chained to
                // this node. This does not add a hash for the chained nodes.
                //再次使用哈希大小，因为节点链接到此节点。这不会为链接的节点添加哈希。
                generateNodeLocalHash(hasher, hashes.size());
            }
        }

        byte[] hash = hasher.hash().asBytes();

        // Make sure that all input nodes have their hash set before entering
        // this loop (calling this method).
        //在进入此循环 (调用此方法) 之前，确保所有输入节点都有其哈希集。
        for (StreamEdge inEdge : node.getInEdges()) {
            byte[] otherHash = hashes.get(inEdge.getSourceId());

            // Sanity check
            if (otherHash == null) {
                throw new IllegalStateException(
                        "Missing hash for input node "
                                + streamGraph.getSourceVertex(inEdge)
                                + ". Cannot generate hash for "
                                + node
                                + ".");
            }

            for (int j = 0; j < hash.length; j++) {
                hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
            }
        }

        if (LOG.isDebugEnabled()) {
            String udfClassName = "";
            if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
                udfClassName =
                        ((UdfStreamOperatorFactory) node.getOperatorFactory())
                                .getUserFunctionClassName();
            }

            LOG.debug(
                    "Generated hash '"
                            + byteToHexString(hash)
                            + "' for node "
                            + "'"
                            + node.toString()
                            + "' {id: "
                            + node.getId()
                            + ", "
                            + "parallelism: "
                            + node.getParallelism()
                            + ", "
                            + "user function: "
                            + udfClassName
                            + "}");
        }

        return hash;
    }

    /**
     * Applies the {@link Hasher} to the {@link StreamNode} . The hasher encapsulates the current
     * state of the hash.
     *
     * <p>The specified ID is local to this node. We cannot use the {@link StreamNode#id}, because
     * it is incremented in a static counter. Therefore, the IDs for identical jobs will otherwise
     * be different.
     */
    private void generateNodeLocalHash(Hasher hasher, int id) {
        // This resolves conflicts for otherwise identical source nodes. BUT
        // the generated hash codes depend on the ordering of the nodes in the
        // stream graph.
        //这解决了在其他方面相同的源节点的冲突。但是生成的哈希码取决于流图中节点的排序。
        hasher.putInt(id);
    }

    private boolean isChainable(
            StreamEdge edge, boolean isChainingEnabled, StreamGraph streamGraph) {
        return isChainingEnabled && StreamingJobGraphGenerator.isChainable(edge, streamGraph);
    }
}
