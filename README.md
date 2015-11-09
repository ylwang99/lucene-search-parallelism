Lucene Search with Parallism
=============
This is a tool to perform Lucene search with inter/intra parallelism implementations.

Getting Started
--------------
1. You can clone the repo with the following command:

	```
	$ git clone git://github.com/ylwang/lucene-search-parallelism.git
	``` 
2. Once you've cloned the repository, change directory into `lucene-parallelism-core` and build the package with Maven:

	```
	$ cd lucene-search-parallelism/lucene-parallelism-core
	$ mvn clean package appassembler:assemble
	```
3. Build index on the entire collection:

	```
	$ sh target/appassembler/bin/IndexStatuses -collection {collectionPath} \
	  -index {indexPath} -optimize
	```

4. Divide the collection into equal size and build index on each of them:

	```
	$ sh target/appassembler/bin/PartitionIndex -collection {collectionPath} \
	  -index {partitionedIndexPath} -parts {# of partitions} -optimize
	```
5. Run Lucene search with inter parallelism:
	```
	$ sh target/appassembler/bin/RunQueriesInterQuery -index {indexPath} \
	  -queries {queryPath} -threads {# of threads}
	```

6. Run Lucene search with intra parallelism:
	```
	$ sh target/appassembler/bin/RunQueriesIntraQuery -index {partitionedIndexPath} \
	  -queries {queryPath} -threads {# of threads}
	```
