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
CXXFLAGS ?= -std=c++20 -O2 -DNDEBUG -Wall -Wextra -MMD -MP -pthread $(PKG_CFLAGS)
LDFLAGS ?= $(PKG_LIBS) $(RPATH_FLAGS) -pthread

BUILD_DIR := build

LIB_SRCS := \
	src/config.cpp \
	src/metrics.cpp \
	src/discovery.cpp \
	src/pooler.cpp \
	src/admin_http.cpp \
	src/admin_grpc.cpp \
	src/client_session.cpp \
	src/net/socket.cpp \
	src/net/tls.cpp \
	src/pg/protocol.cpp \
	src/pg/auth.cpp

PB_SRCS := src/pb/airdb.pb.cc src/pb/airdb.grpc.pb.cc

LIB_OBJS := $(patsubst %.cpp,$(BUILD_DIR)/%.o,$(LIB_SRCS))
PB_OBJS := $(patsubst %.cc,$(BUILD_DIR)/%.o,$(PB_SRCS))
MAIN_OBJ := $(BUILD_DIR)/src/main.o
TEST_OBJ := $(BUILD_DIR)/tests/gateway_test.o
DEPS := $(LIB_OBJS:.o=.d) $(MAIN_OBJ:.o=.d) $(TEST_OBJ:.o=.d)

all: air-gateway

$(BUILD_DIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

$(BUILD_DIR)/%.o: %.cc
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -Isrc -c $< -o $@

air-gateway: $(MAIN_OBJ) $(LIB_OBJS) $(PB_OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test: gateway_test
	./gateway_test

gateway_test: $(TEST_OBJ) $(LIB_OBJS) $(PB_OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

-include $(DEPS)

clean:
	rm -rf $(BUILD_DIR) air-gateway gateway_test
