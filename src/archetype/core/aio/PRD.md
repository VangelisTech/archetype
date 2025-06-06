# Product Requirements Document: Archetype AIO Module with Episode System

  Executive Summary

  The Archetype AIO (Asynchronous Input/Output) module represents a fundamental architectural evolution from synchronous Dict[str,DataFrame] processing to streaming, episode-based concurrent simulation
  execution. This system leverages the natural isolation properties of archetype tables to achieve true parallelism while maintaining temporal consistency through flexible episode coordination.

  Problem Statement

  Current Limitations of Core Module

  1. Synchronous Bottleneck: Sequential processor execution with Dict[str,DataFrame] aggregation
  2. Global Lock-Step: All archetypes must complete before any can advance
  3. Resource Underutilization: CPUs idle waiting for slowest archetype
  4. Memory Pressure: All archetype data loaded simultaneously
  5. Scaling Friction: Adding processors increases coordination overhead

  Business Impact

  - Simulation throughput limited by slowest archetype
  - Poor resource utilization on multi-core systems
  - Memory constraints limit simulation scale
  - Development velocity hampered by synchronous testing cycles

  Solution Architecture

  Core Innovation: Episode-Based Temporal Coordination

  Episodes replace rigid step-based synchronization with flexible temporal boundaries:

```python
  class Episode(Component):
      id: str                          # Unique episode identifier
      start_step: int                  # Episode beginning
      end_step: Optional[int]          # Episode completion
      duration: timedelta              # Real-world duration
      completed_archetypes: Set[str]   # Tracking completion
      synchronization_required: bool   # Force coordination point
```

  Key Architectural Principles

  1. Archetype Isolation: Each archetype processes independently within episodes
  2. Lazy Synchronization: Coordination only at episode boundaries when required
  3. Streaming Semantics: Data flows through processors as available
  4. Backpressure Control: Semaphores prevent resource exhaustion
  5. Temporal Flexibility: Variable episode sizes per archetype type

  Functional Requirements

  FR-1: AsyncProcessor Framework

  Priority: P0

```python
  @async_processor(Position, Velocity, priority=1)
  class AsyncMovementProcessor(AsyncProcessor):
      async def process_stream(self, archetype_name: str, df: DataFrame, dt: float) -> DataFrame:
          # Async operations with semaphore control
          async with self.io_semaphore:
              result = await external_api_call(df)
          return df.with_columns(result)
```

  Acceptance Criteria:
  - Processors execute concurrently per archetype
  - Semaphore-controlled I/O operations
  - Priority-based ordering within archetypes
  - Exception isolation prevents cascade failures

  FR-2: Episode Coordination System

  Priority: P0

```python
  coordinator = EpisodeCoordinator(episode_size=10)

  # Archetypes progress independently
  episode_a = await coordinator.get_episode_for_archetype("arch_a", step=15)
  episode_b = await coordinator.get_episode_for_archetype("arch_b", step=23)

  # Optional synchronization
  await coordinator.wait_for_episode_completion(
      episode_id="ep_2",
      required_archetypes={"critical_arch_1", "critical_arch_2"},
      timeout=30.0
  )
```

  Acceptance Criteria:
  - Independent archetype progression
  - Configurable synchronization points
  - Timeout handling for deadlock prevention
  - Episode metadata tracking

  FR-3: Streaming Store Interface

  Priority: P0

```python
  class AsyncArchetypeStore:
      async def async_get_archetypes_stream(
          self, *component_types: Type[Component]
      ) -> AsyncGenerator[Tuple[str, DataFrame], None]:
          # Yield archetype DataFrames as available

      async def async_append_episode(
          self, archetype_name: str, df: DataFrame, episode: Episode
      ) -> None:
          # Append with episode metadata
```

  Acceptance Criteria:
  - Non-blocking archetype streaming
  - Episode-aware persistence
  - Concurrent read/write safety
  - Memory-efficient streaming

  FR-4: Multiple Execution Strategies

  Priority: P1

  1. Pure Streaming: Process archetypes individually as available
  2. Batched Streaming: Configurable batch sizes for throughput optimization
  3. Episode Coordination: Temporal boundaries with optional synchronization

  Acceptance Criteria:
  - Strategy selection at runtime
  - Performance characteristics documented
  - Graceful degradation under load

  FR-5: Async Query Management

  Priority: P0

  async def query_with_history(
      self, *components: Type[Component],
      episodes: List[str],
      streaming: bool = True
  ) -> AsyncGenerator[DataFrame, None]:
      # Stream historical data across episodes

  Acceptance Criteria:
  - Episode-aware temporal queries
  - Streaming and batch query modes
  - Efficient historical data access

  Non-Functional Requirements

  NFR-1: Performance

  - Throughput: 10x improvement over synchronous execution for I/O-bound processors
  - Latency: Sub-millisecond episode coordination overhead
  - Memory: Constant memory usage regardless of simulation size
  - CPU Utilization: >90% on multi-core systems

  NFR-2: Scalability

  - Concurrent Archetypes: Support 1000+ concurrent archetype streams
  - Processor Count: Linear scaling up to 100 processors per system
  - Episode Size: Configurable from 1 to 10,000 steps
  - Temporal Windows: Handle simulations spanning years of simulated time

  NFR-3: Reliability

  - Fault Isolation: Processor failures don't cascade
  - Graceful Degradation: System continues with reduced processors
  - Episode Recovery: Resume from last completed episode
  - Deadlock Prevention: Timeout mechanisms prevent hangs

  NFR-4: Observability

  - Episode Metrics: Completion rates, timing, synchronization waits
  - Processor Metrics: Throughput, error rates, resource usage
  - System Health: Queue depths, memory pressure, CPU utilization
  - Tracing: End-to-end request tracing through episode pipelines

  Technical Specifications

  Integration with Daft

  @daft.udf(return_dtype=...)
  def async_inference_udf(archetype_data):
      semaphore = asyncio.Semaphore(max_concurrent_requests)

      async def process_with_semaphore(item):
          async with semaphore:
              return await llm_inference(item)

      tasks = [process_with_semaphore(item) for item in archetype_data]
      return asyncio.run(asyncio.gather(*tasks))

  Temporal Consistency Model

  - Within Episode: Full consistency, deterministic ordering
  - Cross Episode: Eventually consistent with coordination points
  - Historical Queries: Point-in-time consistency per episode

  Memory Management

  - Streaming Buffer: Configurable buffer sizes per archetype
  - Lazy Materialization: DataFrames computed only when needed
  - Garbage Collection: Episode data cleaned after persistence

  Migration Strategy

  Phase 1: Core Infrastructure (4 weeks)

  - AsyncProcessor base classes
  - Episode coordination system
  - Basic streaming store interface

  Phase 2: Execution Engines (3 weeks)

  - Pure streaming system
  - Batched streaming system
  - Episode-based system

  Phase 3: Integration & Optimization (3 weeks)

  - Async query management
  - Performance tuning
  - Observability tooling

  Phase 4: Production Readiness (2 weeks)

  - Comprehensive testing
  - Documentation
  - Migration utilities

  Success Metrics

  Performance KPIs

  - Simulation Throughput: 10x improvement for async workloads
  - Resource Utilization: >90% CPU utilization sustained
  - Memory Efficiency: Constant memory usage scaling
  - Latency P99: <100ms episode coordination overhead

  Reliability KPIs

  - Episode Completion Rate: >99.9% successful episode completions
  - Fault Recovery Time: <5 seconds average recovery
  - Data Consistency: Zero consistency violations in temporal queries

  Developer Experience KPIs

  - Migration Effort: <1 day to convert synchronous processors
  - Debug Time: 50% reduction in debugging async issues
  - Test Coverage: >95% code coverage with async test utilities

  Risk Mitigation

  Technical Risks

  - Deadlock Scenarios: Comprehensive timeout mechanisms
  - Memory Leaks: Rigorous testing with long-running simulations
  - Data Races: Careful isolation design and testing
  - Performance Regression: Extensive benchmarking suite

  Business Risks

  - Migration Complexity: Phased rollout with fallback mechanisms
  - Learning Curve: Comprehensive documentation and examples
  - Production Stability: Gradual feature flag deployment

  Conclusion

  The Archetype AIO module with Episode System represents a paradigm shift toward truly scalable, concurrent simulation execution. By leveraging the natural isolation of archetype tables and introducing
  flexible temporal coordination, this system eliminates the fundamental bottlenecks of synchronous execution while maintaining the deterministic behavior required for scientific simulation.

  The episode-based approach provides the perfect abstraction for managing temporal consistency without sacrificing performance, enabling simulations to scale from laptop development to distributed cluster
  execution with the same codebase.