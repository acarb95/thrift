#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
AUTOMAKE_OPTIONS = subdir-objects serial-tests


AM_CFLAGS = -g -Wall -Wextra -pedantic $(GLIB_CFLAGS) $(GOBJECT_CFLAGS) @GCOV_CFLAGS@
AM_CPPFLAGS = -I$(top_srcdir)/lib/c_glib/src -Igen-c_glib -I$(top_srcdir)/../obj/lib
AM_LDFLAGS = $(GLIB_LIBS) $(GOBJECT_LIBS) @GCOV_LDFLAGS@ -L../../../ -lbluebridge

THRIFT = $(top_builddir)/compiler/cpp/thrift

noinst_LTLIBRARIES = \
	libremotememtestgencglib.la \
	libsimplearraycompgencglib.la

nodist_libremotememtestgencglib_la_SOURCES = \
	gen-c_glib/remote_memory_test.c \
	gen-c_glib/remote_memory_test.h \
	gen-c_glib/shared_types.c \
	gen-c_glib/shared_types.h \
	gen-c_glib/remote_memory_test_types.c \
	gen-c_glib/remote_memory_test_types.h 

nodist_libsimplearraycompgencglib_la_SOURCES = \
	gen-c_glib/simple_array_computation.c \
	gen-c_glib/simple_array_computation.h \
	gen-c_glib/shared_types.c \
	gen-c_glib/shared_types.h \
	gen-c_glib/simple_array_computation_types.c \
	gen-c_glib/simple_array_computation_types.h

libremotememtestgencglib_la_LIBADD = \
	$(top_builddir)/lib/c_glib/libthrift_c_glib.la

libsimplearraycompgencglib_la_LIBADD = \
	$(top_builddir)/lib/c_glib/libthrift_c_glib.la


noinst_PROGRAMS = \
	thrift_remote_mem_test_server \
	thrift_simple_array_comp_server \
	thrift_client

thrift_remote_mem_test_server_SOURCES = \
	c_glib_remote_mem_test_server.c

thrift_simple_array_comp_server_SOURCES = \
	c_glib_simple_array_comp_server.c

thrift_remote_mem_test_server_LDADD = \
	libremotememtestgencglib.la \
	$(top_builddir)/lib/c_glib/libthrift_c_glib.la

thrift_simple_array_comp_server_LDADD = \
	libsimplearraycompgencglib.la \
	$(top_builddir)/lib/c_glib/libthrift_c_glib.la

thrift_client_SOURCES = \
	c_glib_client.c

thrift_client_LDADD = \
	libremotememtestgencglib.la \
	libsimplearraycompgencglib.la \
	$(top_builddir)/lib/c_glib/libthrift_c_glib.la


$(nodist_libremotememtestgencglib_la_SOURCES): $(top_srcdir)/tutorial_thrift/RemoteMemoryTest.thrift
	$(THRIFT) --gen c_glib -r $<

$(nodist_libsimplearraycompgencglib_la_SOURCES): $(top_srcdir)/tutorial_thrift/SimpleArrayComputation.thrift
	$(THRIFT) --gen c_glib -r $<


clean-local:
	$(RM) gen-c_glib/*

thriftremmemserver: all
	./thrift_remote_mem_test_server

thriftarrcompserver: all
	./thrift_simple_array_comp_server

thriftclient: all
	./thrift_client

EXTRA_DIST = \
	c_glib_remote_mem_test_server.c \
	c_glib_simple_array_comp_server.c \
	c_glib_client.c
