MAKE_ROOT:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
C_GLIB_DIR = $(MAKE_ROOT)/lib/c_glib
BIFROST_TUTORIAL = $(MAKE_ROOT)/tutorial/c_glib
THRIFT_TUTORIAL = $(MAKE_ROOT)/tutorial_thrift/c_glib

MAKE_ROOT:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
srcExt = c

all:
	@$(MAKE) -C $(C_GLIB_DIR)
	@$(MAKE) -C $(BIFROST_TUTORIAL)
	@$(MAKE) -C $(THRIFT_TUTORIAL)
	#-rm -rf $(objDir)

clean:
	@echo "Cleaning..."
	@$(MAKE) -C $(C_GLIB_DIR) clean
	@$(MAKE) -C $(BIFROST_TUTORIAL) clean
	@$(MAKE) -C $(THRIFT_TUTORIAL) clean
