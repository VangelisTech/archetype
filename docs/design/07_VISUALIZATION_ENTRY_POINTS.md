

The end goal of archetype is to simplify self-service simulation development, generation, and running by ai so that we can bring experimentation nearest to the people with the problem.

In order for naive users to leverage archetype, they will need to be confident that the system implemented their request to their specification.

There is an iterative process initially where a user works with this simulation architect where the user will want a visual aid to understand how the architect chose to implement the request.

Some entry points for visualization include:

- df.explain() - A utility already built in to daft to visualize the physical and logical plans before dataframe materialization. This is integral feature for inspecting how lazy evaluation has interpretted the aggregated expressions/functions/transformations. df.explain() has two options, outputting a text based log, or a mermaid diagram. If I think about how archetype is setup up, the first place this would be useful is right before materialization is executed and the dataframe is sent through the updater. The updater itself is responsible for writing the data to storage (persistance), so a debug flag could be used to investigate df.explain and print the output. The second place this would be useful would be at the end of each processor.process() method, so that the user can witness how the plan is developed across each processor.

- Processor Priority DAG, an analogous, but not equivalent opportunity would be a simpler visualization of the dag topology of processors for a given step. The naive implementation of this is a simple sequential pipeline by priority, but since processors can also add commands to the broker or include logical/conditional transformations, some processors may not be included in the dag at all on some steps while present in others.  A snapshot of the dag at each step would be super helpful, with an image being the most easy to read.

- Command Queue Visualization: Each step processors and external processes can submit commands to the world which are queued in a broker. Regardless of the order of these commands, since they come with a priority, we materialize both state and behavior mutations (implemented as data and processor commands) in the order of priority. Most usecases probably don't care, but a way to visualize the command queue at any given step would be super helpful.

- Visualizing the component schemas as a data model: While not all components will be directly related to eachother in a schema, it would still be helpful to have a data model diagram like what PowerBI has.

- Components and Processors as Block Diagrams: Taking the data model and processor dag pipelines a step futher, Imagine a sankey diagram where interfaces would represent different processors and the thickness of each bar would represent the volume of data being passed in between (number of enities). One could select these interface connector bars with mouse to see which components are included.

A few notes on inspirations:
- A lot of my references for how I would like to debug or develop archetype simulations comes from my experience with simulink. The block diagram interface with subsytems nesting and ports is a natural way to draw control diagrams. While it took a couple of months to get used to, I really enjoyed working in block diagrams everyday and found having to switch between code and block diagrams somewhat combersome. Block diagrams are great for understanding how multiplexed systems are wired together. A mental model is much easier to develop visually rather than abstractly in code sometimes.

At the same time, block diagrams can be a real pain to maintain, and if the underlying logic isn't easily auto-codeable, then their utility loses out pretty quickly. At the end of the day, developing software in block diagrams is just another way to develop software and comes with similar maintanence requirements as normal code.

There are plenty o f


## Moving on from architectural visualization to the underlying data:

Archetype lets you query state using the world.get_archetype() or world.get_archetype_for_entity() methods. Processors are preconfigured to process particular components using the cls.components property set by the @processor decorator. While visualizng how all of the pieces of a simulation are wired together to create a query plan is great and all, at the end of the day, you will want to visualize the actual underlying data.

Assuming your simulation works, and is built in the way you want it to, the best time to query state is after  simulation is completed. Luckily since the core simulatino engine is built with a distributed analytics query engine this is pretty much trivial. I think it would be worthwhile to spend some time developing helper classes to abstract the joins across archetype tables for different table views. The archetype table persistance strategy minimizes data duplication so materializing new tables is not recommended. Even still, when it comes to visualizing data for a given simulation, or across simulations, it may be worthwhile to improve performance with intermediate view materialization. Think executive dashboards on top level aggregations or commonly used pivot tables.

Leaning further into our AI-native approach, none of these details should eventually matter to the user. My goal with archetype has always been to help the end user get the data they need, when they need it. Often this means making a few assumptions up front to apply extra filters and limits on queries. The idea here is that unless you are running a production job, you should almost never need a non filtered/limited query on any data.

Joins are expensive, but thanks to the columnar zero-copy in-memory format of apache arrow we can rest assured that we are achieving the best cost perfomance for each query. It is then desirable to have an expert llm agent that can script daft dataframe queries and inspect tables using the catalog api.

At the end of the day, everything should be accessible, at least when RBAC and data governance are less concerned.

Introducing these complexities means queries need to be routed through the broker that will evaluate prompts through the auth, rbac, and guardrails that gatekeep world methods.
