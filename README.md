# SimpleDB

In this project, made SimpleDB to run in parallel using multiple processes. These processes may all be on a single machine or may be spread across multiple physical machines! If you have access to multiple machines, you may find it fun to test the latter. Otherwise, you can complete the whole assignment on a single machine by running multiple processes on it.

A parallel database management system that runs in a cluster of independent servers is said to follow a shared-nothing architecture.

The main tasks of:

*   Implementing parts of a basic worker process for parallel query processing. In a shared-nothing database management system, multiple workers execute in parallel while exchanging data with each other to compute a query's output.
*   Implementing a special operator called shuffle to enable SimpleDB to run joins in parallel.
*   Implementing an optimized parallel aggregation operator.

These three tasks will expose to three core aspects of parallel data processing: 

*  (1) executing queries using more than one process, 
*  (2) exchanging data between processes for efficient parallel processing, and 
*  (3) optimizing operators for a parallel architecture.
