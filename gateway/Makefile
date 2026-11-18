CXX ?= g++
PKG_CONFIG_PATH_EXT := /opt/conda/lib/pkgconfig:$(PKG_CONFIG_PATH)
PKG_CFLAGS := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH_EXT) pkg-config --cflags libpq openssl grpc++ protobuf 2>/dev/null)
PKG_LIBS := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH_EXT) pkg-config --libs libpq openssl grpc++ protobuf 2>/dev/null)
ifeq ($(strip $(PKG_CFLAGS)),)
PKG_CFLAGS := -I/usr/include/postgresql -I/opt/conda/include
endif
ifeq ($(strip $(PKG_LIBS)),)
PKG_LIBS := -lpq -lssl -lcrypto -lgrpc++ -lprotobuf
endif
ifneq ($(wildcard /opt/conda/lib),)
RPATH_FLAGS := -Wl,-rpath,/opt/conda/lib
else
RPATH_FLAGS :=
endif
CXXFLAGS ?= -std=c++20 -O2 -DNDEBUG -Wall -Wextra -pthread $(PKG_CFLAGS)
LDFLAGS ?= $(PKG_LIBS) $(RPATH_FLAGS) -pthread

PB_SRCS := src/pb/airdb.pb.cc src/pb/airdb.grpc.pb.cc
HEADERS := src/config.hpp src/pg_wire.hpp src/discovery.hpp src/pooler.hpp src/admin_http.hpp src/admin_grpc.hpp src/metrics.hpp

all: air-gateway

air-gateway: src/main.cpp $(PB_SRCS) $(HEADERS)
	$(CXX) $(CXXFLAGS) -Isrc -o $@ src/main.cpp $(PB_SRCS) $(LDFLAGS)

test: gateway_test
	./gateway_test

gateway_test: tests/gateway_test.cpp $(PB_SRCS) $(HEADERS)
	$(CXX) $(CXXFLAGS) -Isrc -o $@ tests/gateway_test.cpp $(PB_SRCS) $(LDFLAGS)

clean:
	rm -f air-gateway gateway_test
