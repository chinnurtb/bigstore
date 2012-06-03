REBAR_URL ?= http://github.com/downloads/basho/rebar/rebar

ifneq ($(shell which wget 2>/dev/null),)
REBAR_GET ?= wget -q $(REBAR_URL)
else
REBAR_GET ?= curl -s -f -L $(REBAR_URL) >rebar
endif

.PHONY: all deps rel tags clean distclean

all: rebar deps
	./rebar compile

rebar:
	$(REBAR_GET)
	chmod +x rebar

deps: rebar
	./rebar get-deps

rel: rebar all
	./rebar generate

tags:
	ctags -R

clean: rebar
	./rebar clean

distclean:
	rm -rf rebar ebin apps/*/ebin deps rel/bigstore tags
