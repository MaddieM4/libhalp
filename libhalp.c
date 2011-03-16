#include <stdio.h>
#include <dirent.h>
#include <string.h>

struct Haddr {
	char* ip;
	int port, time;
}

/*struct Hpos {
	struct Haddr address;
	char* label;
	int pos;
}*/

struct Link {
	void* contents;
	Link* previous;
}

struct HaddrList {
	struct Haddr* items;
	char* label;
	int count;
}

char cachefolder[150];

int init(char* cacheLocation) {
	strlcopy(cacheLocation, cachefolder, 148);
	if (cachefolder[strlen(cachefolder)-1] !='/') {
		cachefolder[strlen(cachefolder)] = '/';
		cachefolder[strlen(cachefolder)+1] = '\0';
	}
	// Test to make sure it's a valid folder
	dp = opendir(cachefolder);
	closedir(dp);
	return dp != NULL;
}

FILE* cacheOpen(const char* label, char mode) {
	return fopen(strncat(cachefolder,label), mode);
}

/*
	get() updates the cache and returns the data that was loaded.
	This doesn't include the full cache, just the updated parts.
*/
struct Hpos[] get(char* label, int s, int e) {
	char buffer[256];
	bzero(buffer,256);
}

/*
	getCache() does not use get() in an effort to prevent infinite loops,
	it uses only the local cache, returning empty list if it is empty or
	does not exist.
*/
struct HaddrList getCache(char* label) {
	// read cache from file
	FILE* cache = cacheOpen(label,'r');
	if (cache==NULL) {
		struct HaddrList empty;
		empty.items = NULL;
		empty.count = 0;
		empty.label = label;
		return empty;
	}
	struct Haddr addr;
	struct Link* link;
	link->previous = NULL;
	link->content = NULL;
	int pos = 0;
	// a quick-and-dirty expanding data structure
	while (fscanf(cache,"%d %s:%d", addr.time,addr.ip,addr.port) == 3) {
		struct Haddr newaddr = addr;
		struct Link currentlink;
		currentlink.contents = &newaddr;
		currentlink.previous = link;
		link = &currentlink;
		pos++;
	}
	// now that we know the number of addresses, we can put them in an array
	struct HaddrList result;
	result.count = pos;
	result.items = Haddr[pos];
	result.label = label;
	while (link->previous != NULL) {
		result.items[--pos] = *((Haddr) link->contents);
		link = link->previous;
	}
	return result;
}

/*
	This will completely overwrite whatever was there, so make sure you
	merge with the existing cache (available through getCache) before you
	save it to disk.
*/
int setCache(HaddrList list) {
	
}

void* connect(char* label, void* (*handler)(int, void**) ) {
	void* null = NULL;
	return connect(label, handler, &null);
}

void* connect(char* label, void* (*handler)(int, void**), void** args) {
	// get a list of all the cached addresses in the label
}
