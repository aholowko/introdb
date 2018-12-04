# requirements

* save and read using 4kB pages

* max record size <= page size

* put a record
  * to the existing last page in file if there is still space
  * or create a new page with the new record

* update a record
  * first find an old existing record and mark it as unused
  * save the record as usual at the end of the file
  
* get a record
  * linear search
  
* remove a record
  * mark as unused
  
* single-threaded implementation
  
# record encoding

marker: byte (1 byte)
keySize: int (4 bytes)
key: byte[] (n bytes)
valueSize: int (4 bytes)
value: byte[] (n bytes)

# tools

$ sync                        # syncs dirty pages with storage
$ sysctl -w vm.drop_caches=1  # invalidates all pages
$ sysctl -w vm.dirty_ratio=80 # 80% of memory can be used for dirty pages
$ sysctl -w vm.dirty_background_ratio=80


# how to unit test

	./mvnw clean test

# how to run JMH

	./mvnw clean package -DskipTests
	java -jar perf/target/benchmarks.jar