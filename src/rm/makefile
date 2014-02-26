
include ../makefile.inc

all: librm.a rmtest 

# lib file dependencies
librm.a: librm.a(rm.o catalog.o pf_interface.o)  # and possibly other .o files

# c file dependencies
*.o: *.cc
rm.o: rm.h rm.cc
pf_interface.o: pf_interface.h  pf_interface.cc
catalog.o: catalog.h catalog.cc
#pftest.o: pftest.cc
rmtest.o: rm.h rm.cc

# binary dependencies
rmtest: rmtest.o catalog.o pf_interface.o librm.a $(CODEROOT)/pf/libpf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a
$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a

.PHONY: clean
clean:
	-rm rmtest *.a *.o *~
	$(MAKE) -C $(CODEROOT)/pf clean
