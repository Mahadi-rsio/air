MODULE_big = air_smgr
OBJS = air_smgr.o communicator.o lfc.o

EXTENSION = air_smgr
DATA = air_smgr--1.0.sql
PGFILEDESC = "air_smgr - AIR Pageserver storage manager for PostgreSQL 17"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
