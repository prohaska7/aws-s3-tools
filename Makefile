SRC = $(wildcard s3*.py)
UTILS = $(patsubst %.py,%,$(SRC))

all: $(UTILS)

clean:
	rm -rf $(UTILS)

install: $(UTILS)
	cp $(UTILS) $(HOME)/bin

%: %.py
	ln $< $@
