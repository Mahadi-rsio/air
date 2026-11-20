MODULE_big = air_ro
OBJS = air_ro.o

EXTENSION = air_ro
DATA = air_ro--1.0.sql
PGFILEDESC = "air_ro - AIR native PostgreSQL 17 read-only execution guard"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
