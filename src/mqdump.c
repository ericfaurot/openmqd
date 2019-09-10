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
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vis.h>

#include "mq.h"

int main_mqdump(int, char **);

static void dump_seginfo(struct mq *);
static void dump_record(struct mq *, struct mq_recinfo *, char *, char *);
static void dump_rawrecord(struct mq *, struct mq_recinfo *, char *, char *);
static const char *strtype(int);

static void
usage(void)
{
	extern const char * __progname;

	fprintf(stderr, "usage: %s [-Rr] <-d dir> <-k key> <-o offset> <-w sec>\n", __progname);
	exit(1);
}

int
main_mqdump(int argc, char **argv)
{
	struct mq_recinfo rec;
	const char *dirname = "/var/mqd/queue";
	struct mq *mq;
	int ch;
	int wdelay = 0, raw = 0, rotate = 1;
	uint64_t offset = 0;
	char *buf = NULL, *key = NULL, *tmp;
	size_t bufsz = 0;

	while ((ch = getopt(argc, argv, "d:k:o:Rrw:")) != -1) {
		switch (ch) {
		case 'd':
			dirname = optarg;
			break;
		case 'k':
			key = optarg;
			break;
		case 'o':
			/* start offset */
			offset = atoi(optarg);
			break;
		case 'R':
			rotate = 0;
			break;
		case 'r':
			raw = 1;
			break;

		case 'w':
			wdelay = atoi(optarg);
			break;
		default:
			usage();
			/* NOTREACHED */
		}
	}
	argc -= optind;
	argv += optind;

	if ((mq = mq_diropen(dirname, offset, MQ_READ)) == NULL)
		err(1, "mq_open: %s", argv[0]);
	if (!raw)
		dump_seginfo(mq);

	for(;;) {

		if (mq_next(mq, &rec) == -1) {
			if (errno == EAGAIN) {
				if (wdelay) {
					sleep(wdelay);
					continue;
				}
				break;
			}
			err(1, "mq_next");
		}

		if (rec.size) {
			if (bufsz < rec.size) {
				tmp = reallocarray(buf, rec.size, 1);
				if (tmp == NULL)
					err(1, "reallocarray");
				buf = tmp;
				bufsz = rec.size;
			}
			if (mq_payload(mq, buf, bufsz) == -1)
				err(1, "reallocarray");
		}
		if (raw)
			dump_rawrecord(mq, &rec, buf, key);
		else
			dump_record(mq, &rec, buf, key);

		if (rec.type == REC_ROTATE) {
			mq_close(mq);
			if ((mq = mq_diropen(dirname, rec.offset, MQ_READ))
			     == NULL)
				err(1, "mq_diropen");
			if (!raw)
				dump_seginfo(mq);
		}
	}

	return 0;
}

static void
dump_seginfo(struct mq *mq)
{
	struct mq_seginfo seg;
	struct tm tm;
	time_t t;
	char buf[80];


	if (mq_info(mq, &seg) == -1)
		err(1, "mq_info");

	printf("SEGMENT\n");

	printf("   version: %d\n", seg.version);
	printf("   flags: 0x%x\n", seg.flags);

	t = seg.basetime.tv_sec;
	gmtime_r(&t, &tm);
	strftime(buf, sizeof(buf), "%F %T", &tm);
	printf("   base offset: %llu\n", (unsigned long long)seg.baseoffset);
	printf("   base time: %s.%06d\n", buf, (int)seg.basetime.tv_usec);

	t = seg.currtime.tv_sec;
	gmtime_r(&t, &tm);
	strftime(buf, sizeof(buf), "%F %T", &tm);
	printf("   current offset: %llu\n", (unsigned long long)seg.curroffset);
	printf("   current time: %s.%06d\n", buf, (int)seg.currtime.tv_usec);
}

static void
dump_record(struct mq *mq, struct mq_recinfo *rec, char *payload,
    char *filterkey)
{
	struct tm tm;
	time_t t;
	char buf[1024];
	char *key = NULL;
	size_t keylen = 0, mlen;

	if (rec->key) {
		key = payload;
		keylen = strlen(key) + 1;
		payload += keylen;
	}

	if (filterkey && (key == NULL || strcmp(key, filterkey)))
		return;

	t = rec->timestamp.tv_sec;
	gmtime_r(&t, &tm);
	strftime(buf, sizeof(buf), "%F %T", &tm);

	printf("%s\n   offset: %lld\n   time: %s.%06d\n   size: %zu\n",
	    strtype(rec->type),
	    rec->offset,
	    buf,
	    (int)rec->timestamp.tv_usec,
	    rec->size);

	if (rec->key) {
		strvisx(buf, key, keylen - 1, VIS_NL | VIS_CSTYLE);
		printf("   key: %s\n", buf);
	}
	mlen = rec->size - keylen;
	if (mlen) {
		if (mlen > 80) {
			memmove(payload + 78, "...", 3);
			mlen = 80;
		}
		strvisx(buf, payload, mlen, VIS_WHITE | VIS_CSTYLE);
		printf("   data: %s\n", buf);
	}
}

static void
dump_rawrecord(struct mq *mq, struct mq_recinfo *rec, char *payload,
    char *filterkey)
{
	char *key = NULL;
	size_t keylen = 0, mlen;

	if (rec->key) {
		key = payload;
		keylen = strlen(key) + 1;
		payload += keylen;
	}
	mlen = rec->size - keylen;
	if (mlen == 0)
		return;

	if (filterkey && (key == NULL || strcmp(key, filterkey)))
		return;

	fwrite(payload, 1, mlen, stdout);
}

static const char *
strtype(int type)
{
	static char buf[64];

	switch(type) {
	case REC_MESSAGE:
		return "MESSAGE";
	case REC_CHECKPOINT:
		return "CHECKPOINT";
	case REC_ROTATE:
		return "ROTATE";
	default:
		snprintf(buf, sizeof(buf), "%d?", type);
		return buf;
	}
}
