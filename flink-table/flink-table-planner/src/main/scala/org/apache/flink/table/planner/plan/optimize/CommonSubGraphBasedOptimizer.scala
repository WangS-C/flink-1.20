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
package org.apache.flink.table.planner.plan.optimize

import org.apache.flink.table.planner.plan.reuse.SubplanReuser
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.plan.utils.SameRelObjectShuttle
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toJava
import org.apache.flink.table.planner.utils.ShortcutUtils.{unwrapContext, unwrapTableConfig, unwrapTypeFactory}

import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan

import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

/**
 * A [[Optimizer]] that optimizes [[RelNode]] DAG into semantically [[RelNode]] DAG based common
 * sub-graph. Common sub-graph represents the common sub RelNode plan in multiple RelNode trees.
 * Calcite planner does not support DAG (multiple roots) optimization, so a [[RelNode]] DAG should
 * be decomposed into multiple common sub-graphs, and each sub-graph is a tree (which has only one
 * root), and can be optimized independently by Calcite [[org.apache.calcite.plan.RelOptPlanner]].
 * The algorithm works as follows:
 *   1. Decompose [[RelNode]] DAG into multiple [[RelNodeBlock]]s, and build [[RelNodeBlock]] DAG.
 *      Each [[RelNodeBlock]] has only one sink output, and represents a common sub-graph. 2.
 *      optimize recursively each [[RelNodeBlock]] from leaf block to root(sink) block, and wrap the
 *      optimized result of non-root block as an [[IntermediateRelTable]]. 3. expand
 *      [[IntermediateRelTable]] into RelNode tree in each [[RelNodeBlock]].
 *
 * Currently, we choose this strategy based on the following considerations:
 *   1. In general, for multi-sinks users tend to use VIEW which is a natural common sub-graph. 2.
 *      after some optimization, like project push-down, filter push-down, there may be no common
 *      sub-graph (even table source) could be found in the final plan. however, current strategy
 *      has some deficiencies that need to be improved:
 *   1. how to find a valid break-point for common sub-graph, e.g. some physical RelNodes are
 *      converted from several logical RelNodes, so the valid break-point could not be between those
 *      logical nodes. 2. the optimization result is local optimal (in sub-graph), not global
 *      optimal.
 */
abstract class CommonSubGraphBasedOptimizer extends Optimizer {

  /** ID for IntermediateRelTable. */
  private val NEXT_ID = new AtomicInteger(0)

  protected def createUniqueIntermediateRelTableName: String = {
    val id = NEXT_ID.getAndIncrement()
    s"IntermediateRelTable_$id"
  }

  /**
   * Generates the optimized [[RelNode]] DAG from the original relational nodes. NOTES: the reused
   * node in result DAG will be converted to the same RelNode, and the result doesn't contain
   * [[IntermediateRelTable]].
   *
   * @param roots
   *   the original relational nodes.
   * @return
   *   a list of RelNode represents an optimized RelNode DAG.
   */
  //生成优化的RelNode来自原始关系节点的DAG。
  //注意: 结果DAG中的重用节点将转换为相同的RelNode，并且结果不包含IntermediateRelTable.。
  override def optimize(roots: Seq[RelNode]): Seq[RelNode] = {
    // resolve hints before optimizing
    //在优化之前解决提示
    val queryHintsResolver = new QueryHintsResolver()
    val resolvedHintRoots = queryHintsResolver.resolve(toJava(roots))

    // clear query block alias bef optimizing
    //清除查询块别名bef优化
    val clearQueryBlockAliasResolver = new ClearQueryBlockAliasResolver
    val resolvedAliasRoots = clearQueryBlockAliasResolver.resolve(resolvedHintRoots)

    val sinkBlocks = doOptimize(resolvedAliasRoots)
    val optimizedPlan = sinkBlocks.map {
      block =>
        val plan = block.getOptimizedPlan
        require(plan != null)
        plan
    }
    //展开每个RelNode块中的中间Rel表
    val expanded = expandIntermediateTableScan(optimizedPlan)

    val postOptimizedPlan = postOptimize(expanded)

    // Rewrite same rel object to different rel objects
    // in order to get the correct dag (dag reuse is based on object not digest)
    //将相同的rel对象重写为不同的rel对象，以获得正确的dag (dag重用基于对象而不是摘要)
    val shuttle = new SameRelObjectShuttle()
    val relsWithoutSameObj = postOptimizedPlan.map(_.accept(shuttle))

    // reuse subplan
    //重用子计划
    SubplanReuser.reuseDuplicatedSubplan(
      relsWithoutSameObj,
      unwrapTableConfig(roots.head),
      unwrapContext(roots.head),
      unwrapTypeFactory(roots.head))
  }

  /**
   * Post process for the physical [[RelNode]] dag, e.g., can be overloaded for validation or
   * rewriting purpose.
   */
  //例如，物理RelNode dag的后处理可以被重载以用于验证或重写目的。
  protected def postOptimize(expanded: Seq[RelNode]): Seq[RelNode] = expanded

  /**
   * Decompose RelNode trees into multiple [[RelNodeBlock]]s, optimize recursively each
   * [[RelNodeBlock]], return optimized [[RelNodeBlock]]s.
   *
   * @return
   *   optimized [[RelNodeBlock]]s.
   */
  //将RelNode树分解为多个RelNodeBlocks，递归优化每个RelNodeBlock,返回优化RelNodeBlocks。
  protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock]

  /** Returns a new table scan which wraps the given IntermediateRelTable. */
  protected def wrapIntermediateRelTableToTableScan(
      relTable: IntermediateRelTable,
      name: String): TableScan = {
    val cluster = relTable.relNode.getCluster
    new LogicalTableScan(cluster, cluster.traitSet, relTable)
  }

  /** Expand [[IntermediateRelTable]] in each RelNodeBlock. */
  //展开每个RelNode块中的中间Rel表。
  private def expandIntermediateTableScan(nodes: Seq[RelNode]): Seq[RelNode] = {

    class ExpandShuttle extends RelShuttleImpl {

      // ensure the same intermediateTable would be expanded to the same RelNode tree.
      //确保相同的intermediateTable将被扩展到相同的RelNode树。
      private val expandedIntermediateTables =
        new util.IdentityHashMap[IntermediateRelTable, RelNode]()

      override def visit(scan: TableScan): RelNode = {
        val intermediateTable = scan.getTable.unwrap(classOf[IntermediateRelTable])
        if (intermediateTable != null) {
          expandedIntermediateTables.getOrElseUpdate(
            intermediateTable, {
              val underlyingRelNode = intermediateTable.relNode
              underlyingRelNode.accept(this)
            })
        } else {
          scan
        }
      }
    }

    val shuttle = new ExpandShuttle
    nodes.map(_.accept(shuttle))
  }

}
