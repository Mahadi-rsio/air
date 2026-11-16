CXX ?= g++
PKG_CFLAGS := $(shell pkg-config --cflags libpq libcurl openssl 2>/dev/null)
PKG_LIBS := $(shell pkg-config --libs libpq libcurl openssl 2>/dev/null)
ifeq ($(strip $(PKG_CFLAGS)),)
PKG_CFLAGS := -I/usr/include/postgresql
endif
ifeq ($(strip $(PKG_LIBS)),)
PKG_LIBS := -lpq -lcurl -lssl -lcrypto
endif
CXXFLAGS ?= -std=c++20 -O2 -Wall -Wextra -pthread $(PKG_CFLAGS)
LDFLAGS ?= $(PKG_LIBS) -pthread

all: air-gateway

air-gateway: src/main.cpp src/config.hpp src/pg_wire.hpp src/discovery.hpp src/pooler.hpp src/admin_http.hpp
	$(CXX) $(CXXFLAGS) -Isrc -o $@ src/main.cpp $(LDFLAGS)

test: gateway_test
	./gateway_test

gateway_test: tests/gateway_test.cpp src/config.hpp src/pg_wire.hpp src/discovery.hpp src/pooler.hpp src/admin_http.hpp
	$(CXX) $(CXXFLAGS) -Isrc -o $@ tests/gateway_test.cpp $(LDFLAGS)

clean:
	rm -f air-gateway gateway_test
