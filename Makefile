MODULE_big = pg_background_queue
OBJS = pg_background_queue.o

EXTENSION = pg_background_queue
DATA = pg_background_queue--1.0.sql
REGRESS = pg_background_queue

PG_CONFIG = /Applications/Postgres.app/Contents/Versions/16/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)