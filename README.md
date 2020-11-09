# Dynamo Metrics

Simplified implementation of Amazon's Dynamo DB in Elixir, along with measurements of the system under different conditions.

## Implementation

The [`emulation` app](apps/emulation) was provided as part of NYU' s Distributed Systems course and contains emulation code necessary for testing with message drops, delays, and more.

The [`dynamo` app](apps/dynamo) contains the actual implementation of Dynamo. Most of the logic is contained in the [`DynamoNode` module,](lib/node.ex) which implements a single Dynamo node.
