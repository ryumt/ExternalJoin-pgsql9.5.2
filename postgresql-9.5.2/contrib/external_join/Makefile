# contrib/external_join/Makefile

MODULE_big = external_join
OBJS = external_join.o $(WIN32RES)
PGFILEDESC = "external_join - working"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/external_join
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

CXX = g++ -I ../../src/backend
CXXFLAGS = -fPIC -std=c++11 -Wno-deprecated-register -Wno-deprecated-declarations -Wall
