.PHONY: clean format profile

CPPFLAGS = -std=c++17 -O3 -Wall -march=native -pipe

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
	compression.h lsm_tree.cpp lsm_tree.h minheap.h
	g++ -o server $(CPPFLAGS) server.cpp merge.cpp compression.cpp lsm_tree.cpp -I. -L. -lsnappy

format:
	clang-format -i *.cpp *.h

profile: server
	sudo perf record --call-graph dwarf ./server queries.dsl
	sudo perf report
