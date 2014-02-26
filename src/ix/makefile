include ../makefile.inc

all: libix.a ixtest

# lib file dependencies
libix.a: libix.a(ix.o btree.o ixpf.o)  # and possibly other .o files

# c file dependencies
*.o: *.cc
ix.o: ix.h ix.cc
ixpf.o: ixpf.h  ixpf.cc
btree.o: btree.h btree.cc
#ix.o: ix.h

ixtest.o: ix.o ixpf.o btree.o

# binary dependencies
ixtest: ixtest.o ix.o ixpf.o btree.o libix.a $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/pf/libpf.a
$(CODEROOT)/pf/libpf.a:
	$(MAKE) -C $(CODEROOT)/pf libpf.a


.PHONY: $(CODEROOT)/pf/librm.a
$(CODEROOT)/rm/librm.a:
	$(MAKE) -C $(CODEROOT)/rm librm.a

.PHONY: clean
clean:
	-rm ixtest *.a *.o *~
	$(MAKE) -C $(CODEROOT)/pf clean
	$(MAKE) -C $(CODEROOT)/rm clean
