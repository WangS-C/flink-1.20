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
package org.apache.flink.table.planner.delegation

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.catalog._
import org.apache.flink.table.catalog.ManagedTableListener.isManagedTable
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.delegation.{Executor, Parser, ParserFactory, Planner}
import org.apache.flink.table.factories.{DynamicTableSinkFactory, FactoryUtil, TableFactoryUtil}
import org.apache.flink.table.module.{Module, ModuleManager}
import org.apache.flink.table.operations._
import org.apache.flink.table.operations.OutputConversionModifyOperation.UpdateMode
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.calcite._
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema
import org.apache.flink.table.planner.connectors.DynamicSinkUtils
import org.apache.flink.table.planner.connectors.DynamicSinkUtils.validateSchemaAndApplyImplicitCast
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalLegacySink
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNodeGraph, ExecNodeGraphGenerator}
import org.apache.flink.table.planner.plan.nodes.exec.processor.{ExecNodeGraphProcessor, ProcessorContext}
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.optimize.Optimizer
import org.apache.flink.table.planner.sinks.DataStreamTableSink
import org.apache.flink.table.planner.sinks.TableSinkUtils.{inferSinkPhysicalSchema, validateLogicalPhysicalTypesCompatible, validateTableSink}
import org.apache.flink.table.planner.utils.InternalConfigOptions.{TABLE_QUERY_CURRENT_DATABASE, TABLE_QUERY_START_EPOCH_TIME, TABLE_QUERY_START_LOCAL_TIME}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.{toJava, toScala}
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.table.runtime.generated.CompileUtils
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter

import _root_.scala.collection.JavaConversions._
import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.{RelTrait, RelTraitDef}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalTableModify

import java.lang.{Long => JLong}
import java.util
import java.util.{Collections, TimeZone}

import scala.collection.mutable

/**
 * Implementation of a [[Planner]]. It supports only streaming use cases. (The new
 * [[org.apache.flink.table.sources.InputFormatTableSource]] should work, but will be handled as
 * streaming sources, and no batch specific optimizations will be applied).
 *
 * @param executor
 *   instance of [[Executor]], needed to extract [[StreamExecutionEnvironment]] for
 *   [[org.apache.flink.table.sources.StreamTableSource.getDataStream]]
 * @param tableConfig
 *   mutable configuration passed from corresponding [[TableEnvironment]]
 * @param moduleManager
 *   manager for modules
 * @param functionCatalog
 *   catalog of functions
 * @param catalogManager
 *   manager of catalog meta objects such as tables, views, databases etc.
 * @param isStreamingMode
 *   Determines if the planner should work in a batch (false}) or streaming (true) mode.
 */
abstract class PlannerBase(
    executor: Executor,
    tableConfig: TableConfig,
    val moduleManager: ModuleManager,
    val functionCatalog: FunctionCatalog,
    val catalogManager: CatalogManager,
    isStreamingMode: Boolean,
    classLoader: ClassLoader)
  extends Planner {

  private var parserFactory: ParserFactory = _
  private var parser: Parser = _
  private var currentDialect: SqlDialect = getTableConfig.getSqlDialect
  // the transformations generated in translateToPlan method, they are not connected
  // with sink transformations but also are needed in the final graph.
  //在translateToPlan方法中生成的转换，它们与接收器转换无关，但在最终图中也需要。
  private[flink] val extraTransformations = new util.ArrayList[Transformation[_]]()

  @VisibleForTesting
  private[flink] val plannerContext: PlannerContext =
    new PlannerContext(
      !isStreamingMode,
      tableConfig,
      moduleManager,
      functionCatalog,
      catalogManager,
      asRootSchema(new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
      getTraitDefs.toList,
      classLoader
    )

  private[flink] def createRelBuilder: FlinkRelBuilder = {
    plannerContext.createRelBuilder()
  }

  @VisibleForTesting
  private[flink] def createFlinkPlanner: FlinkPlannerImpl = {
    plannerContext.createFlinkPlanner()
  }

  private[flink] def getTypeFactory: FlinkTypeFactory = plannerContext.getTypeFactory

  protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]]

  protected def getOptimizer: Optimizer

  def getTableConfig: TableConfig = tableConfig

  def getFlinkContext: FlinkContext = plannerContext.getFlinkContext

  /**
   * @deprecated
   *   Do not use this method anymore. Use [[getTableConfig]] to access options. Create
   *   transformations without it. A [[StreamExecutionEnvironment]] is a mixture of executor and
   *   stream graph generator/builder. In the long term, we would like to avoid the need for it in
   *   the planner module.
   */
  @deprecated
  private[flink] def getExecEnv: StreamExecutionEnvironment = {
    executor.asInstanceOf[DefaultExecutor].getExecutionEnvironment
  }

  def getParserFactory: ParserFactory = {
    if (parserFactory == null || getTableConfig.getSqlDialect != currentDialect) {
      val factoryIdentifier = getTableConfig.getSqlDialect.name().toLowerCase
      parserFactory = FactoryUtil.discoverFactory(
        getClass.getClassLoader,
        classOf[ParserFactory],
        factoryIdentifier)
      currentDialect = getTableConfig.getSqlDialect
      parser = null
    }
    parserFactory
  }

  override def getParser: Parser = {
    if (parser == null || getTableConfig.getSqlDialect != currentDialect) {
      parserFactory = getParserFactory
      parser = parserFactory.create(new DefaultCalciteContext(catalogManager, plannerContext))
    }
    parser
  }

  override def translate(
      modifyOperations: util.List[ModifyOperation]): util.List[Transformation[_]] = {
    //前期准备
    beforeTranslation()
    if (modifyOperations.isEmpty) {
      return List.empty[Transformation[_]]
    }

    //转换为rel
    val relNodes = modifyOperations.map(translateToRel)
    //优化rel
    val optimizedRelNodes = optimize(relNodes)
    //转换为ExecNodeGraph
    val execGraph = translateToExecNodeGraph(optimizedRelNodes, isCompiled = false)
    //翻译一个ExecNodeGraph变成一个Transformation DAG。
    val transformations = translateToPlan(execGraph)
    afterTranslation()
    transformations
  }

  /** Converts a relational tree of [[ModifyOperation]] into a Calcite relational expression. */
  //将ModifyOperation的关系树转换为Calcite关系表达式。
  @VisibleForTesting
  private[flink] def translateToRel(modifyOperation: ModifyOperation): RelNode = {
    val dataTypeFactory = catalogManager.getDataTypeFactory
    modifyOperation match {
      case s: UnregisteredSinkModifyOperation[_] =>
        val input = createRelBuilder.queryOperation(s.getChild).build()
        val sinkSchema = s.getSink.getTableSchema
        // validate query schema and sink schema, and apply cast if possible
        //验证查询架构和接收器架构，并在可能的情况下应用强制转换
        val query = validateSchemaAndApplyImplicitCast(
          input,
          catalogManager.getSchemaResolver.resolve(sinkSchema.toSchema),
          null,
          dataTypeFactory,
          getTypeFactory)
        LogicalLegacySink.create(
          query,
          s.getSink,
          "UnregisteredSink",
          ConnectorCatalogTable.sink(s.getSink, !isStreamingMode))

      case collectModifyOperation: CollectModifyOperation =>
        val input = createRelBuilder.queryOperation(modifyOperation.getChild).build()
        //将tablelesult.collect() 的接收器转换为RelNode。
        DynamicSinkUtils.convertCollectToRel(
          createRelBuilder,
          input,
          collectModifyOperation,
          getTableConfig,
          getFlinkContext.getClassLoader
        )

      case stagedSink: StagedSinkModifyOperation =>
        val input = createRelBuilder.queryOperation(modifyOperation.getChild).build()
        //将给定的DynamicTableSink转换为RelNode。如果需要，它会添加辅助投影。
        DynamicSinkUtils.convertSinkToRel(
          createRelBuilder,
          input,
          stagedSink,
          stagedSink.getDynamicTableSink)

      case catalogSink: SinkModifyOperation =>
        val input = createRelBuilder.queryOperation(modifyOperation.getChild).build()
        val dynamicOptions = catalogSink.getDynamicOptions
        getTableSink(catalogSink.getContextResolvedTable, dynamicOptions).map {
          case (table, sink: TableSink[_]) =>
            // Legacy tables can't be anonymous
            //旧表不能是匿名的
            val identifier = catalogSink.getContextResolvedTable.getIdentifier

            // check it's not for UPDATE/DELETE because they're not supported for Legacy table
            //检查它不是UPDATE DELETE，因为它们不支持旧版表
            if (catalogSink.isDelete || catalogSink.isUpdate) {
              throw new TableException(
                String.format(
                  "Can't perform %s operation of the table %s " +
                    " because the corresponding table sink is the legacy TableSink," +
                    " Please implement %s for it.",
                  if (catalogSink.isDelete) "delete" else "update",
                  identifier,
                  classOf[DynamicTableSink].getName
                ))
            }

            // check the logical field type and physical field type are compatible
            //检查逻辑字段类型和物理字段类型是否兼容
            val queryLogicalType = FlinkTypeFactory.toLogicalRowType(input.getRowType)
            // validate logical schema and physical schema are compatible
            //验证逻辑架构和物理架构是否兼容
            validateLogicalPhysicalTypesCompatible(table, sink, queryLogicalType)
            // validate TableSink
            //验证 TableSink
            validateTableSink(catalogSink, identifier, sink, table.getPartitionKeys)
            // validate query schema and sink schema, and apply cast if possible
            //验证查询架构和接收器架构，并在可能的情况下应用强制转换
            val query = validateSchemaAndApplyImplicitCast(
              input,
              table.getResolvedSchema,
              identifier.asSummaryString,
              dataTypeFactory,
              getTypeFactory)
            val hints = new util.ArrayList[RelHint]
            if (!dynamicOptions.isEmpty) {
              hints.add(RelHint.builder("OPTIONS").hintOptions(dynamicOptions).build)
            }
            LogicalLegacySink.create(
              query,
              hints,
              sink,
              identifier.toString,
              table,
              catalogSink.getStaticPartitions.toMap)

          case (table, sink: DynamicTableSink) =>
            //将给定的DynamicTableSink转换为RelNode
            DynamicSinkUtils.convertSinkToRel(createRelBuilder, input, catalogSink, sink)
        } match {
          case Some(sinkRel) => sinkRel
          case None =>
            throw new TableException(
              s"Sink '${catalogSink.getContextResolvedTable}' does not exists")
        }

      case externalModifyOperation: ExternalModifyOperation =>
        val input = createRelBuilder.queryOperation(modifyOperation.getChild).build()
        //将外部接收器 (即进一步的数据流转换) 转换为RelNode。
        DynamicSinkUtils.convertExternalToRel(createRelBuilder, input, externalModifyOperation)

      // legacy
      //遗留问题
      case outputConversion: OutputConversionModifyOperation =>
        val input = createRelBuilder.queryOperation(outputConversion.getChild).build()
        val (needUpdateBefore, withChangeFlag) = outputConversion.getUpdateMode match {
          case UpdateMode.RETRACT => (true, true)
          case UpdateMode.APPEND => (false, false)
          case UpdateMode.UPSERT => (false, true)
        }
        val typeInfo = LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(outputConversion.getType)
        val inputLogicalType = FlinkTypeFactory.toLogicalRowType(input.getRowType)
        val sinkPhysicalSchema =
          inferSinkPhysicalSchema(outputConversion.getType, inputLogicalType, withChangeFlag)
        // validate query schema and sink schema, and apply cast if possible
        //验证查询架构和接收器架构，并在可能的情况下应用强制转换
        val query = validateSchemaAndApplyImplicitCast(
          input,
          catalogManager.getSchemaResolver.resolve(sinkPhysicalSchema.toSchema),
          null,
          dataTypeFactory,
          getTypeFactory)
        val tableSink = new DataStreamTableSink(
          FlinkTypeFactory.toTableSchema(query.getRowType),
          typeInfo,
          needUpdateBefore,
          withChangeFlag)
        LogicalLegacySink.create(
          query,
          tableSink,
          "DataStreamTableSink",
          ConnectorCatalogTable.sink(tableSink, !isStreamingMode))

      case _ =>
        throw new TableException(s"Unsupported ModifyOperation: $modifyOperation")
    }
  }

  @VisibleForTesting
  private[flink] def optimize(relNodes: Seq[RelNode]): Seq[RelNode] = {
    //生成优化的RelNode
    val optimizedRelNodes = getOptimizer.optimize(relNodes)
    require(optimizedRelNodes.size == relNodes.size)
    optimizedRelNodes
  }

  @VisibleForTesting
  private[flink] def optimize(relNode: RelNode): RelNode = {
    val optimizedRelNodes = getOptimizer.optimize(Seq(relNode))
    require(optimizedRelNodes.size == 1)
    optimizedRelNodes.head
  }

  /**
   * Converts [[FlinkPhysicalRel]] DAG to [[ExecNodeGraph]], tries to reuse duplicate sub-plans and
   * transforms the graph based on the given processors.
   */
  //将FlinkPhysicalRel DAG转换为execnodeggraph ，尝试重用重复的子计划，并根据给定的处理器转换图。
  @VisibleForTesting
  private[flink] def translateToExecNodeGraph(
      optimizedRelNodes: Seq[RelNode],
      isCompiled: Boolean): ExecNodeGraph = {
    val nonPhysicalRel = optimizedRelNodes.filterNot(_.isInstanceOf[FlinkPhysicalRel])
    if (nonPhysicalRel.nonEmpty) {
      throw new TableException(
        "The expected optimized plan is FlinkPhysicalRel plan, " +
          s"actual plan is ${nonPhysicalRel.head.getClass.getSimpleName} plan.")
    }

    require(optimizedRelNodes.forall(_.isInstanceOf[FlinkPhysicalRel]))

    // convert FlinkPhysicalRel DAG to ExecNodeGraph
    //将FlinkPhysicalRel DAG转换为execnodegrah
    val generator = new ExecNodeGraphGenerator()
    val execGraph = {
      //生成ExecNodeGraph
      generator.generate(optimizedRelNodes.map(_.asInstanceOf[FlinkPhysicalRel]), isCompiled)
    }

    // process the graph
    //处理graph
    val context = new ProcessorContext(this)
    val processors = getExecNodeGraphProcessors
    //给定execnodeggraph ，处理它并返回结果execnodeggraph。
    processors.foldLeft(execGraph)((graph, processor) => processor.process(graph, context))
  }

  protected def getExecNodeGraphProcessors: Seq[ExecNodeGraphProcessor]

  /**
   * Translates an [[ExecNodeGraph]] into a [[Transformation]] DAG.
   *
   * @param execGraph
   *   The node graph to translate.
   * @return
   *   The [[Transformation]] DAG that corresponds to the node DAG.
   */
  //翻译一个ExecNodeGraph变成一个Transformation DAG。
  protected def translateToPlan(execGraph: ExecNodeGraph): util.List[Transformation[_]]

  def addExtraTransformation(transformation: Transformation[_]): Unit = {
    if (!extraTransformations.contains(transformation)) {
      extraTransformations.add(transformation)
    }
  }

  private def getTableSink(
      contextResolvedTable: ContextResolvedTable,
      dynamicOptions: JMap[String, String]): Option[(ResolvedCatalogTable, Any)] = {
    contextResolvedTable.getTable[CatalogBaseTable] match {
      case connectorTable: ConnectorCatalogTable[_, _] =>
        val resolvedTable = contextResolvedTable.getResolvedTable[ResolvedCatalogTable]
        toScala(connectorTable.getTableSink) match {
          case Some(sink) => Some(resolvedTable, sink)
          case None => None
        }

      case regularTable: CatalogTable =>
        val resolvedTable = contextResolvedTable.getResolvedTable[ResolvedCatalogTable]
        val tableToFind = if (dynamicOptions.nonEmpty) {
          resolvedTable.copy(FlinkHints.mergeTableOptions(dynamicOptions, resolvedTable.getOptions))
        } else {
          resolvedTable
        }
        val catalog = toScala(contextResolvedTable.getCatalog)
        val objectIdentifier = contextResolvedTable.getIdentifier
        val isTemporary = contextResolvedTable.isTemporary

        if (
          isStreamingMode && isManagedTable(catalog.orNull, resolvedTable) &&
          !executor.isCheckpointingEnabled
        ) {
          throw new TableException(
            s"You should enable the checkpointing for sinking to managed table " +
              s"'$contextResolvedTable', managed table relies on checkpoint to commit and " +
              s"the data is visible only after commit.")
        }

        if (
          !contextResolvedTable.isAnonymous &&
          TableFactoryUtil.isLegacyConnectorOptions(
            catalogManager.getCatalog(objectIdentifier.getCatalogName).orElse(null),
            tableConfig,
            isStreamingMode,
            objectIdentifier,
            resolvedTable,
            isTemporary
          )
        ) {
          val tableSink = TableFactoryUtil.findAndCreateTableSink(
            catalog.orNull,
            objectIdentifier,
            tableToFind,
            getTableConfig,
            isStreamingMode,
            isTemporary)
          Option(resolvedTable, tableSink)
        } else {
          val factoryFromCatalog = catalog.flatMap(f => toScala(f.getFactory)) match {
            case Some(f: DynamicTableSinkFactory) => Some(f)
            case _ => None
          }

          val factoryFromModule = toScala(
            plannerContext.getFlinkContext.getModuleManager
              .getFactory(toJava((m: Module) => m.getTableSinkFactory)))

          // Since the catalog is more specific, we give it precedence over a factory provided by
          // any modules.
          val factory = factoryFromCatalog.orElse(factoryFromModule).orNull

          val tableSink = FactoryUtil.createDynamicTableSink(
            factory,
            objectIdentifier,
            tableToFind,
            Collections.emptyMap(),
            getTableConfig,
            getFlinkContext.getClassLoader,
            isTemporary)
          Option(resolvedTable, tableSink)
        }

      case _ => None
    }
  }

  protected def createSerdeContext: SerdeContext = {
    val planner = createFlinkPlanner
    new SerdeContext(
      getParser,
      planner.config.getContext.asInstanceOf[FlinkContext],
      plannerContext.getTypeFactory,
      planner.operatorTable
    )
  }

  protected def beforeTranslation(): Unit = {
    // Add query start time to TableConfig, these config are used internally,
    // these configs will be used by temporal functions like CURRENT_TIMESTAMP,LOCALTIMESTAMP.
    //将查询开始时间添加到TableConfig，这些配置在内部使用，
    // 这些配置将由时间函数使用，如CURRENT_TIMESTAMP，LOCALTIMESTAMP。
    val epochTime: JLong = System.currentTimeMillis()
    tableConfig.set(TABLE_QUERY_START_EPOCH_TIME, epochTime)
    val localTime: JLong = epochTime +
      TimeZone.getTimeZone(TableConfigUtils.getLocalTimeZone(tableConfig)).getOffset(epochTime)
    tableConfig.set(TABLE_QUERY_START_LOCAL_TIME, localTime)

    val currentDatabase = Option(catalogManager.getCurrentDatabase).getOrElse("")
    tableConfig.set(TABLE_QUERY_CURRENT_DATABASE, currentDatabase)

    // We pass only the configuration to avoid reconfiguration with the rootConfiguration
    //我们仅传递配置以避免使用 rootConfiguration 重新配置
    getExecEnv.configure(tableConfig.getConfiguration, classLoader)

    // Use config parallelism to override env parallelism.
    //使用配置并行性来覆盖环境并行性。
    val defaultParallelism =
      getTableConfig.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM)
    if (defaultParallelism > 0) {
      getExecEnv.getConfig.setParallelism(defaultParallelism)
    }
  }

  protected def afterTranslation(): Unit = {
    // Cleanup all internal configuration after plan translation finished.
    //完成计划转换后，清除所有内部配置。
    val configuration = tableConfig.getConfiguration
    configuration.removeConfig(TABLE_QUERY_START_EPOCH_TIME)
    configuration.removeConfig(TABLE_QUERY_START_LOCAL_TIME)
    configuration.removeConfig(TABLE_QUERY_CURRENT_DATABASE)

    // Clean caches that might have filled up during optimization
    //清理优化过程中可能填满的缓存
    CompileUtils.cleanUp()
    extraTransformations.clear()
  }

  /** Returns all the graphs required to execute EXPLAIN */
  private[flink] def getExplainGraphs(operations: util.List[Operation])
      : (mutable.Buffer[RelNode], Seq[RelNode], ExecNodeGraph, StreamGraph) = {
    require(operations.nonEmpty, "operations should not be empty")
    beforeTranslation()
    val sinkRelNodes = operations.map {
      case queryOperation: QueryOperation =>
        val relNode = createRelBuilder.queryOperation(queryOperation).build()
        relNode match {
          // SQL: explain plan for insert into xx
          case modify: LogicalTableModify =>
            // convert LogicalTableModify to SinkModifyOperation
            val qualifiedName = modify.getTable.getQualifiedName
            require(qualifiedName.size() == 3, "the length of qualified name should be 3.")
            val objectIdentifier =
              ObjectIdentifier.of(qualifiedName.get(0), qualifiedName.get(1), qualifiedName.get(2))
            val contextResolvedTable = catalogManager.getTableOrError(objectIdentifier)
            val modifyOperation = new SinkModifyOperation(
              contextResolvedTable,
              new PlannerQueryOperation(
                modify.getInput,
                () => queryOperation.asSerializableString())
            )
            translateToRel(modifyOperation)
          case _ =>
            relNode
        }
      case modifyOperation: ModifyOperation =>
        translateToRel(modifyOperation)
      case o => throw new TableException(s"Unsupported operation: ${o.getClass.getCanonicalName}")
    }
    val optimizedRelNodes = optimize(sinkRelNodes)
    val execGraph = translateToExecNodeGraph(optimizedRelNodes, isCompiled = false)
    val transformations = translateToPlan(execGraph)
    afterTranslation()

    // We pass only the configuration to avoid reconfiguration with the rootConfiguration
    val streamGraph = executor
      .createPipeline(transformations, tableConfig.getConfiguration, null)
      .asInstanceOf[StreamGraph]

    (sinkRelNodes, optimizedRelNodes, execGraph, streamGraph)
  }
}
