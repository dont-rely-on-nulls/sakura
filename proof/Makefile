COQMAKEFILE ?= Makefile.coq

all: $(COQMAKEFILE)
	mkdir -p ../lib/drl_verified
	$(MAKE) -f $(COQMAKEFILE)

$(COQMAKEFILE): _CoqProject
	coq_makefile -f _CoqProject -o $(COQMAKEFILE)

clean: $(COQMAKEFILE)
	$(MAKE) -f $(COQMAKEFILE) clean
	rm -f $(COQMAKEFILE) $(COQMAKEFILE).conf

.PHONY: all clean
