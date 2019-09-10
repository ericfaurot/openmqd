/*	$OpenBSD$	*/
/*
 * Copyright (c) 2017 Eric Faurot <eric@openbsd.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <err.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mq.h"

int main_mqtest(int, char **);

__dead static void
usage(void)
{
	printf("usage: %s [-d dirname] [-k key]\n", getprogname());
	exit(0);
}

int
main_mqtest(int argc, char **argv)
{
	struct mq *mq;
	const char *dirname = "/var/mqd/queue";
	const char *key = NULL, *errstr = NULL;
	char *line = NULL;
	size_t linesize = 0;
	ssize_t linelen;
	uint64_t offset;
	int ch, verbose = 0, r, msgcount, segcount, strip = 0;
	struct mq_seginfo seg;
	size_t maxsize;
	size_t maxreloffset;
	size_t maxreltime;

	maxsize = mq_getmaxsize(NULL);
	maxreloffset = mq_getmaxreloffset(NULL);
	maxreltime = mq_getmaxreltime(NULL);

	while ((ch = getopt(argc, argv, "d:k:O:sS:T:v")) != -1) {
		switch (ch) {
		case 'd':
			dirname = optarg;
			break;
		case 'k':
			key = optarg;
			break;
		case 'O':
			maxreloffset = strtonum(optarg, 1, maxreloffset, &errstr);
			if (errstr)
				err(1, "maxreloffset");
		case 's':
			strip = 1;
			break;
		case 'S':
			maxsize = strtonum(optarg, 512, maxsize, &errstr);
			if (errstr)
				err(1, "maxsize");
			break;
		case 'T':
			maxreltime = strtonum(optarg, 1, maxreltime, &errstr);
			if (errstr)
				err(1, "maxreltime");
			break;
		case 'v':
			verbose++;
			break;
		default:
			usage();
		}
	}
	argc -= optind;
	argv += optind;

	if (mq_scan(dirname, &offset) == -1)
		err(1, "mq_scan");
	if ((mq = mq_diropen(dirname, offset, MQ_WRITE)) == NULL)
		err(1, "mq_diropen");
	if (mq_setmaxsize(mq, maxsize) == -1)
		err(1, "mq_setmaxsize");
	if (mq_setmaxreloffset(mq, maxreloffset) == -1)
		err(1, "mq_setmaxreloffset");
	if (mq_setmaxreltime(mq, maxreltime) == -1)
		err(1, "mq_setmaxreltime");
	if (mq_info(mq, &seg) == -1)
		err(1, "mq_info");
	if (verbose) {
		printf("base offset %llu\n", seg.baseoffset);
		printf("current offset %llu\n", seg.curroffset);
	}

	segcount = 0;
	msgcount = 0;
	while ((linelen = getline(&line, &linesize, stdin)) != -1) {
		if (strip && line[linelen] == '\n') {
			line[linelen] = '\0';
			linelen--;
		}
		while ((r = mq_push(mq, 0, key, linelen, line)) == 0) {
			mq_rotate(mq, &offset);
			if (verbose)
				printf("rotating queue at offset %llu\n", offset);
			if ((mq = mq_diropen(dirname, offset, MQ_WRITE)) == NULL)
				err(1, "mq_diropen");
			if (mq_setmaxsize(mq, maxsize) == -1)
				err(1, "mq_setmaxsize");
			if (mq_setmaxreloffset(mq, maxreloffset) == -1)
				err(1, "mq_setmaxreloffset");
			if (mq_setmaxreltime(mq, maxreltime) == -1)
				err(1, "mq_setmaxreltime");
			segcount += 1;
		}
		if (r == -1)
			err(1, "mq_push");
		msgcount++;
        }
	if (mq_info(mq, &seg) == -1)
		err(1, "mq_info");
	if (verbose) {
		printf("final offset %llu\n", seg.curroffset);
		printf("%d messages, %d rotations\n", msgcount, segcount);
	}

	mq_close(mq);

	return 0;
}
