ROOT = ../..
PROJECT = kazoo_tasks

all: compile

test: csvs

csvs:
	`stat -t -- test/*.csv >/dev/null 2>/dev/null || ./download_csvs.py`
#	./download_csvs.py

include $(ROOT)/make/kz.mk
