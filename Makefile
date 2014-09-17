all: atlas1 atlas2 atlas

atlas1:
	./fgr2.py placement --partition atlas1 --numranks 1008
	./fgr2.py placement --partition atlas1 --numranks 1008 --strategy random
atlas2:
	./fgr2.py placement --partition atlas2 --numranks 1008
	./fgr2.py placement --partition atlas2 --numranks 1008 --strategy random
atlas:
	./fgr2.py placement --partition atlas --numranks 2016
	./fgr2.py placement --partition atlas --numranks 2016 --strategy random
clean:
	rm -f *.log
	rm -f *.debug
	rm -f atlas*.sh

