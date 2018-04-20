SRC = $(wildcard s3*.py s3*.sh)
UTILS = $(patsubst %.sh,%,$(patsubst %.py,%,$(SRC)))

all: $(UTILS)

clean:
	rm -rf $(UTILS)

install: $(UTILS)
	cp $(UTILS) $(HOME)/bin

%: %.py
	ln $< $@
