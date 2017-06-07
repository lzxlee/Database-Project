# Database-Project
Completed project for Berkeley's databases course

### Package Overview

* At the top level, all code is contained within the `database` package. The
  `Database` class is simply the integration of table and query package methods
  that ties everything together. These packages are described below.
* The `databox` package provides a `DataBox` interface that encapsulates data
  values. We've provided implementations for the four data types we'll be
  dealing with in this project (`int`, `float`, `boolean`, and `String`). These
  data containers know how to serialize and deserialize themselves for storing
  as byte arrays.
* The `io` package provides you a fully-fledged paging system and buffer
  manager. The two interfaces you will want to
  understand are the `Page` interface and the `PageAllocator` interface. The
  `Page` interface allows you to manipulate bytes in a particular `Page`.
  `PageAllocator` will allow you to allocate new pages and fetch pages in a
  file.  Don't stress if you don't understand everything that's happening in
  the code in these files. We've tried to document it as well as we can, but
  some things are pretty complicated. Just make sure you understand how to
  consume the existing interfaces!
* The `table` package provides you the beginning of an implementation of
  relational database tables. We've set up some basics for you, like creating a
  `PageAllocator` for each table. We've also provided some helper methods like
  `writeBitToHeader` that you're probably going to want to use! Part 1 of the
  project will be finishing up this table implementation. **Please don't change
  the interfaces in any of these classes, or your tests won't pass.**
* Within the `table` package, you'll also find a `stats` package. This package
  gives you a simple set of statistics for each table (`numRecords`,
  `Histogram`'s, etc.).
* The `index` package provides you the a B+ tree implementation. A `BPlusTree`
  comprises of `BPlusNodes`, which are either `InnerNodes` or `LeafNodes`.
  Nodes contain entries, respectively `BEntry`, `InnerEntry`, and `LeafEntry`.
  If a superclass has a method that is not implemented, that is a sign to
  implement it at the subclass (e.g. `InnerNode` and `LeafNode` instead of
  `BNode`).
* The `query` package provides you a query processing implementation and query
  generation interface. The `QueryOperator` provides an interface for a bunch
  of different operators. In later projects, you will be extending this to
  implement more efficient operators. For now, you can use the existing
  operators, which are implemented such that they fully materialize all tuples
  in memory before processing them. For this project, you will mainly concern
  yourself with the `QueryPlan` interface. The methods in `QueryPlan` will
  allow you to easily generate queries.
* The `concurrency` package provides a lock manager for coordinating concurrent
  database transactions.
  
### Part 1: Schemas, Records, and Tables

The first part of the project involves completing the implementations of
`Schema` and `Table` that we've started for you. Like we said earlier, you will
be implementing **fixed-length records** in this section. Booleans, integers,
and floats are already of fixed-length, so this primarily affects our
`StringDataBox` implementation. Whenever you create a `StringDataBox` field,
you will need to specify the number of bytes that should be reserved for that
string.

#### 1.1 Schemas

You'll need to implement the `Schema#verify`, `Schema#encode`, and
`Schema#decode` methods. The contracts provided by these methods are fully
explained by the JavaDocs. For the `encode` method, you might find the
`java.nio.ByteBuffer` class useful; for `decode`, you might find
`Arrays.copyOfRange` useful.

#### 1.2 Creating and Retrieving Records

Once you've finished `Schema`, you should start by implementing
`Table#addRecord` to add a new `Record` to the table.

However, even before implementing `addRecord`, you'll have to do some
arithmetic in `Table#setEntryCounts` to figure out exactly how many entries are
on a page. Pages in our system are a fixed size `Page.pageSize`, which is
currently set to 4KB. Since each record has a fixed size, you should be able to
pretty easily figure out the optimal number of records that can be stored on a
page. Make sure you account for the entry header (one bit (not byte) per
record!).

We then suggest you implement `Table#checkRecordIDValidity` as it will be
useful for the rest of the `Table` class. The last part of this chunk is
implementing `Table#getRecord`. This should allow you to pass some of the basic
`Table` tests that we've provided.

#### 1.3 Updating and Deleting Records

The next thing you'll need to do in this section is implement
`Table#updateRecord` and `Table#deleteRecord`. Once you've implemented these
methods, you should be passing all of the non-iterator `Table` tests.

#### 1.4 Iterators

To finish up this section, you will need to implement the `TableIterator`
subclass we've started for you within the `Table` class. To make your life
easier, we've provided you with a `PageIterator` in `PageAllocator` that lets
you iterate over all pages. All you have to do is return the valid records from
each page. Remember that Page 0, the first page returned by `PageIterator`, is
reserved for the table header.

### Part 2: B+ Trees

#### 2.1 Inserting Keys

You will first implement B+ tree key insertion. In order to support key
insertion, you will first have to implement the methods `BPlusTree#insertKey`,
`InnerNode#insertBEntry`, and `LeafNode#insertBEntry`.

Recall that if a node is full when inserting a key, you will need to split the
node. Implement the `InnerNode#splitNode` and `LeafNode#splitNode` methods.
Remember to keep the B+ tree key invariance and that when dealing with splits,
we copy keys up from the leaf node and push keys up from the inner node.

Be sure to call `BPlusTree#updateRoot` if the root node has been split. The
root node may be either a `LeafNode` or `InnerNode`.

#### 2.2 Iterators

Similar to Project 1, you will implement the `BPlusIterator` subclass of
`BPlusTree`. This iterator will be a little different from the `TableIterator`,
however. You will need to support equality lookups, bounded range lookups, and
full index scans with this iterator, which are respectively called from
`BPlusTree#lookupKey`, `BPlusTree#sortedScanFrom`, and `BPlusTree#sortedScan`.
Think about how to reuse your code for each case.

### Part 3: Query Operators

All join operators are subclasses of `JoinOperator`, and all operators are subclasses of `QueryOperator`.
We have provided `SelectOperator`, `GroupByOperator`, `SequentialScanOperator`, and `SNLJOperator` as references.

#### 3.1 Join Operators

First, take a look at `SNLJOperator` to understand how join operators are implemented.
Then, implement the iterators for `PNLJOperator`, `BNLJOperator`, `GraceHashOperator`, and
`SortMergeOperator` as described in lecture. It is recommended you review the lecture slides
before getting down and dirty with the code. Think carefully about what instance variables you need before you begin!

For `SortMergeOperator`, you may want to consider implementing a helper function that sorts
a single relation (though not required).

#### 3.2 Index Scan Operator

Next, implement an iterator for `IndexScanOperator` that supports the predicates (`EQUALS`,
 `LESS_THAN`, `LESS_THAN_EQUALS`, `GREATER_THAN`, `GREATER_THAN_EQUALS`) on
the given index.
These predicates are part of the enum `PredicateOperator` found in the class `QueryPlan`.

### Part 4: Query Optimization

This project is focused on the query optimizer. We have provided some structure to help you get started. The `QueryPlan#executeOptimal` method runs the query optimizer and has been implemented already. This method calls other methods which you'll need to implement in order to make sure the optimizer works correctly.

As an overview of what this method does, it first searches for the lowest cost ways to access each single table in the query. Then using the dynamic programming search algorithm covered in lecture, it will try to compute a join between a set of tables and a new table if there exists a join condition between them. The lowest cost join of all the tables is found, and then a group by and select operator is applied on top if those are specified in the query. The method returns an iterator over the final operator created. The search should only consider left-deep join trees and avoid Cartesian products.**Note that we are not expecting you to consider "interesting orders" for the purposes of this project.**

For an example of the naive query plan generation, look at the code inside `QueryPlan#execute()`. Note that the query optimizer code will look quite different from the naive code, but the naive code still serves as a good example of how to compose a query plan tree.

#### 4.1 Cost estimation

The first part of building the query optimizer is ensuring that each query operator has the appropriate IO cost estimates. In order to estimate IO costs for each query operator, you will need the table statistics for any input operators. This information is accessible from the `QueryOperator#getStats` method. The `TableStats` object returned represents estimated statistics of the operator's output, including information such as number of tuples and number of pages in the output among others. These statistics are generated whenever a `QueryOperator` is constructed.

All of the logic for estimating statistics has been fully implemented except for the calculation of the reduction factor of a `SELECT` predicate. You must implement the `IntHistogram#computeReductionFactor` method which will return a reduction factor based on the type of predicate given. The reduction factor calculations should be the same as those that were taught in class.

After implementing this method, you should be passing all of the tests in `TestIntHistogram`.

Each type of `QueryOperator` has a different `estimateIOCost` method which handles IO cost estimation. You will be implementing this method in a few of the operators. This method should estimate the IO cost of executing a query plan rooted at that query operator. It is executed whenever a `QueryOperator` is constructed, and afterwards the cost of an operator can be accessed from the `QueryOperator#getIOCost` method.

Several operators already have their `estimateIOCost` methods implemented. In this project, you are only responsible for implementing this method in `IndexScanOperator`, `SNLJOperator`, `PNLJOperator`, `BNLJOperator`, and `GraceHashOperator`. For the index scan cost, assume an unclustered index is used. The methods in `Transaction` and `TableStats#getReductionFactor` will be useful for implementing the index scan cost. For the grace hash join cost, assume there is only one phase of partitioning.

After implementing the methods in this section, you should be passing all of the tests in `TestQueryPlanCosts`. And you should now have everything you need to start building the search algorithm.

#### 4.2 Single table access selection (Pass 1)

The first part of the search algorithm involves finding the lowest cost plans for accessing each single table in the query. You will be implementing this functionality in `QueryPlan#minCostSingleAccess`. This method takes in a single table name and should first calculate the estimated IO cost of performing a sequential scan. Then if there are any eligible indices that can be used to scan the table, it should calculate the estimated IO cost of performing such an index scan. The `QueryPlan#getEligibleIndexColumns` method can be used to determine whether there are any existing indices that can be used for this query. Out of all of these operators, keep track of the lowest cost operator.

Then as part of a heuristic-based optimization we covered in class, you should push down any selections that correspond to the table. You should be implementing the push down select functionality in `QueryPlan#pushDownSelects` which will be called by the `QueryPlan#minCostSingleAccess` method.

The end result of this method should be a query operator that starts with either a `SequentialScanOperator` or `IndexScanOperator` followed by zero or more `SelectOperator`'s.

After implementing all the methods up to this point, you should be passing all of the tests in `TestOptimalQueryPlan`. These tests do not involve any joins.

#### 4.3 Join selection (Pass i > 1)

The next part of the search algorithm involves finding the lowest cost join between each set of tables in the previous pass and a separate single table. You will be implementing this functionality in `QueryPlan#minCostJoins`. Remember to only consider left-deep plans and to avoid creating any Cartesian products. Use the list of explicit join conditions added through the `QueryPlan#join` method to identify potential joins. Once you've identified a potential join between a left set of tables and a right table, you should be considering each type of join implementation in `QueryPlan#minCostJoinType` which will be called by the `QueryPlan#minCostJoins` method.

The end result of this method should be a mapping from a set of tables to a join query operator that corresponds to the lowest cost join estimated.

