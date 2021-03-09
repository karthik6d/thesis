.PHONY: clean format profile

CPPFLAGS = -std=c++17 -O3 -Wall -march=native -pipe
OBJECTS= SIMDCompressionAndIntersection/codecfactory.o SIMDCompressionAndIntersection/bitpacking.o SIMDCompressionAndIntersection/integratedbitpacking.o SIMDCompressionAndIntersection/simdbitpacking.o SIMDCompressionAndIntersection/usimdbitpacking.o SIMDCompressionAndIntersection/simdintegratedbitpacking.o SIMDCompressionAndIntersection/intersection.o SIMDCompressionAndIntersection/varintdecode.o SIMDCompressionAndIntersection/streamvbyte.o SIMDCompressionAndIntersection/simdpackedsearch.o SIMDCompressionAndIntersection/simdpackedselect.o SIMDCompressionAndIntersection/frameofreference.o SIMDCompressionAndIntersection/for.o

all: basic

basic: basic.cpp
	g++ -g -o basic $(CPPFLAGS) basic.cpp $(OBJECTS) -I/usr/local/opt/zlib/include -lz -Iinclude -I. -L. -lsnappy -lzstd
