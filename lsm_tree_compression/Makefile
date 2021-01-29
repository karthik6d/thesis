.PHONY: clean format profile

CPPFLAGS = -std=c++17 -O3 -Wall -march=native -pipe
OBJECTS= SIMDCompressionAndIntersection/codecfactory.o SIMDCompressionAndIntersection/bitpacking.o SIMDCompressionAndIntersection/integratedbitpacking.o SIMDCompressionAndIntersection/simdbitpacking.o SIMDCompressionAndIntersection/usimdbitpacking.o SIMDCompressionAndIntersection/simdintegratedbitpacking.o SIMDCompressionAndIntersection/intersection.o SIMDCompressionAndIntersection/varintdecode.o SIMDCompressionAndIntersection/streamvbyte.o SIMDCompressionAndIntersection/simdpackedsearch.o SIMDCompressionAndIntersection/simdpackedselect.o SIMDCompressionAndIntersection/frameofreference.o SIMDCompressionAndIntersection/for.o

all: gen server

clean:
	rm -f gen
	rm -f *.csv generator/*.csv
	rm -f *.dsl generator/*.dsl
	rm -f *.exp generator/*.exp
	rm -f server
	rm -rf data
	rm -f *.res
	rm -f perf.data*
	rm -rf enc

gen: generator/generator.go
	cd generator && go build . && mv generator ../gen

server: server.cpp server.h merge.cpp merge.h compression.cpp \
	compression.h lsm_tree.cpp lsm_tree.h minheap.h bloom_filter.hpp
	g++ -g -o server $(CPPFLAGS) server.cpp merge.cpp compression.cpp lsm_tree.cpp $(OBJECTS) -Iinclude -I. -L. -lsnappy

format:
	clang-format -i *.cpp *.h

profile: server
	sudo perf record --call-graph dwarf ./server queries.dsl
	sudo perf report
