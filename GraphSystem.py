
class GraphSystem(BaseSystem):
    """
    Executes processors across a topologically sorted directed acyclic graph. 

    There are two types of topologies: 
    - Physical: Describes how data is related eachother
    - Logical: Describes processor execution order. 

    Processors operate on the DataFrames updating the data nodes and adding or removing edges. 

    The goal of this GraphSystem is to provide a flexible interface for describing the logical execution order of processors over a knowledge graph. 
    Naturally, processors have no knowledge of eachother and are unaware of the graph structure.  
    The GraphSystem is responsible for maintaining the graph state and executing the processors in the correct order. 
    This also means we need special processors that are identifiable 

    Example: 
        onsider a knowledge graph with N nodes and E edges.  
        Processors group components together in order to perform computations.  
        The combined component dataframes contain all entities for the given step. This means the 
        Another way of thinking about these result dataframes is that they are a join of composites. 
        Naturally, some results contain columns that are not present in the original components. 
        These are filtered out later before materialization.

        We can then think of the knowledge graph's nodes as the processors results. 

        The point of a graph system is to provide a flexible means of describing the logical execution order of these processors.

        The graph system will then topologically sort the processors and execute them in the desired order, identifying which processors are decoupled  parallelizable.  
  


    """
    @inject
    def __init__(self, world: 'World' = Provide[SystemContainer.world]):
        self.world = world
        self.nodes: daft.DataFrame = None
        self.edges: daft.DataFrame = None
        self.results: Dict[Processor, daft.DataFrame] = {}


        self.edges: 

        self.graph = nx.DiGraph()



    def add_node(self, processor: Processor) -> None:
        """Adds an entity state processor"""
        if not isinstance(processor, Processor):
            raise TypeError("Can only add Processor instances.")
        
        self._graph.add_node(processor)

    def add_nodes(self, processor: Processor, priority: Optional[int] = None) -> None:
        """Adds a processor instance."""
        if not isinstance(processor, Processor):
            raise TypeError("Can only add Processor instances.")
        
    def remove_node(self, processor: Processor) -> None:
        """Removes a processor instance."""
        if not isinstance(processor, Processor):
            raise TypeError("Can only remove Processor instances.")
        
        self._graph.remove_node(processor)

    def add_edges(self, processor: Processor, dependencies: List[Type[Processor]]) -> None:
        """Adds edges representing dependencies between processors."""
        if processor not in self._graph:
            raise ValueError(f"Processor {processor} not in graph.")
        for dep in dependencies:
            # Assuming dependencies are processor *types* or instances already in the graph
            # You might need a way to resolve types to instances if they aren't already nodes
            if dep not in self._graph:
                 raise ValueError(f"Dependency {dep} not in graph.")
            self._graph.add_edge(dep, processor) # Edge from dependency to processor

    def get_subgraph(self, processors: List[Type[Processor]]) -> nx.DiGraph:
        """Returns a subgraph containing the specified processors."""
        # Ensure nodes exist before creating subgraph
        nodes_in_graph = [p for p in processors if p in self._graph]
        return self._graph.subgraph(nodes_in_graph)
    
    def get_node(self, processor_instance: Processor) -> Processor:
        """Gets the processor instance if it exists in the graph."""
        if processor_instance in self._graph:
            return processor_instance
        raise ValueError(f"Processor {processor_instance} not found in graph.")
    
    def get_neighbors(self, processor_instance: Processor) -> List[Processor]:
        """Gets the neighbors (predecessors and successors) of a processor."""
        if processor_instance not in self._graph:
            raise ValueError(f"Processor {processor_instance} not found in graph.")
        # networkx neighbors includes both predecessors and successors
        return list(self._graph.neighbors(processor_instance))
    
    def execute_processors(self):
        # Implementation depends on how you want to execute the graph
        # (e.g., topological sort for sequential execution of parallelizable steps)
        pass

    def execute_connectivity_function(self):
        pass

    
    
        
        
