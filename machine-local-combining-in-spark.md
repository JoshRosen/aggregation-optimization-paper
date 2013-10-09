# Machine-local combining of reduce inputs in Spark

## Problem

In current versions of Spark, `reduceByKey` shuffles `O(numMapPartitions * numReducers)` blocks over the network.  In most cases, it would be more efficient to perform a machine-local reduction of all of the blocks processed by a particular machine, since this may drastically reduce the amount of data we need to shuffle.

In the current implementation, `reduceByKey` performance can degrade when a large number of map partitions are used: we may transfer `O(numMapPartitions * numReducePartitions)` blocks when, ideally, we could transfer only `O(numMachines * numReducePartitions)`.


## Goals

Our solution should:

- allow for flexible, just-in-time scheduling of map tasks (i.e. dynamic load-balancing),
- support flexible map task scheduling during recovery,
- not require us to know the number of physical machines that participate in producing the reduce phases' input, and
- have no parameters that need tuning. 

## Invariants

There are two invariants that we can exploit in order to cleanly implement machine-local reducers:

1. The pre-shuffle reduce inputs are never consumed one-at-a-time: a reducer always reads an entire set of reduce inputs corresponding to one reduce partition before it produces any output.
2. A mapper loses either all or none of the reduce input blocks that it produced for a particular reduce phase.



## Algorithm

We'll use the BlockManager to store metadata about which blocks have been locally reduced, and merge mew map outputs with existing stored map outputs from the same job:

```
def mapTask(partitionId, data, isSpeculative) {
    // Produce the map outputs:
    mapOutputs = f(data)
    // Store those map outputs in the local block manager:
    combinerInfo = blockManager.getOrElseCreateLocalBlock("reduce_stage_%i_info" % stage)
    synchronized (combinerInfo) {
        if (combinerInfo.block != null && !isSpeculative) {
            combinerBlock = blockManager.get(combinerInfo.block)
            synchronized (combinerBlock) {
                if (!combinerBlock.wasSent) {
                    // Merge into the existing block:
                    combinerBlock.accumulate(mapOutput)
                    // Store a dummy block:
                    blockManager.put("map_output_" + partitionId, [])
                }
            }
        }
        // Store the map output regularly:
        combinerInfo.block = "map_output_" + partitionId
        blockManager.put(mapOutputs, combinerInfo.block,
                         StorageLevel.MemoryAndDisk)
    }
}    
```

The key idea here is that map tasks belonging to the same job will discover other local map outputs corresponding to the same job and accumulate their changes into those blocks.  The block that's the current target for accumulation is discovered by querying a local metadata block that's named after the map phase identifier.

The reduce tasks have no knowledge of whether the map outputs have been combined or not.  A reducer still attempts to fetch all `O(numMapPartitions)` of its input blocks, but many of those blocks will be empty if their contents have been combined into other blocks.

If a node fails, all of the map tasks that ran on that machine will have to be re-run.  These map tasks may be scheduled on several different machines.  Reducers will attempt to fetch the output of the failed mappers by referencing those blocks' names.  So those blocks may have been empty dummy blocks in the original recompilation, but we'll need to make sure that the recomputed output is stored to one of those blocks.  If we naively accumulated the recomputed outputs to the machine-local aggregates of other nodes, we would risk both under- and over-counting of that map task's reduce contribution.

To handle this case, we disallow additional accumulation into blocks that have been fetched by reducers.  Note that this still allows for machine-local reduction when recomputing the lost blocks: if those tasks are scheduled on the same machine, then this allows those blocks to be combined.  This also supports a form of asynchronous, pipelined reduce: reducers can request any blocks that have already been computed, and as soon as those blocks are fetched, a new local accumulation starts to avoid miscounting.

Speculative map tasks could also lead to double-counting, so we choose to not locally aggregate the outputs of speculative tasks.

What if a mapper crashes after sending only some of its output, such as a bunch of empty blocks?  The reducers have already marked those blocks as 'received', so they won't ever re-request them. I _think_ this is only a problem during pipelining, but it's potentially destructive to this entire approach.  Basically, when a machine fails, we can't assume that all of the outputs that it produced will be re-requested.  To fix this, we could defer making the dummy blocks visible until the end of the job.  But this is also problematic, since it messes with our progress indicators and we don't have a great way to identify the last task that's going to be run on a machine during its map phase.  We could solve this by making a rule that require reducers to re-request all empty blocks whenever any map tasks fail.  But this seems a bit heavy-handed.  

But wait!  Maybe this isn't an issue after all: when that mapper fails, either the map task itself has a bug and is doomed (so we just add a rule to flush the accumulation buffer), or the entire machine failed, so every task on that machine has to be re-run anyways.

On the other hand: the failed task will certainly be recomputed if that mapper also happened to run a reducer (since it needs to recompute its local contribution to its own reduce task), but it might not get fetched in case we're unlucky enough to use the "dummy" map result as the accumulator.

Instead of sending an empty block, the reducer sends a manifest saying which partitions should be in the block (alternatively, where that block should be found).  If the block isn't found there, we attempt to re-fetch it.

TODO: HAT IF MAPPERS AND REDUCERS ARE DISTNCT?

### Proof of correctness

Proposed Invariants:

- Once a reducer has received all of its inputs: it will never ask for any blocks.
- Outputs destined to different machines can be treated differently.  We can always assume that we have a single reducer and just trivially generalize the problem to multiple reducers by arguing that they operate on disjoint data.

## Limitations

- If the local combining crashes or fails, we lose the output of all map tasks run on that machine.

## TODO

- Fix the speculation edge-case that limits the combining (minor lack of clarity above, not a technical issue)