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
#include <sys/types.h>
#include <sys/time.h>

#define MSG_MAXDATALEN 	(1 << 24)	/* 16M is largely enough for a message */
#define MSG_MAXKEYLEN 	(1 << 10)

#define MQ_READ		1
#define MQ_WRITE	2

struct mq;

struct mq_seginfo {
	int		version;
	int		flags;
	uint64_t	baseoffset;
	struct timeval	basetime;
	uint64_t	curroffset;
	struct timeval	currtime;
};

#define REC_MESSAGE	0
#define REC_CHECKPOINT	1
#define REC_ROTATE	2
#define REC_MSGBATCH	3

struct mq_recinfo {
	int		type;
	int		key;
	uint64_t	offset;
	struct timeval	timestamp;
	size_t		size;
};

/* General */
int mq_timestamp(uint64_t *);
int mq_validkey(const char *);
int mq_scan(const char *, uint64_t *);
int mq_open(const char *, uint64_t, int);
int mq_unlink(const char *, uint64_t);

/* Queue management */
struct mq *mq_fdopen(int, uint64_t, int);
struct mq *mq_diropen(const char *, uint64_t, int);
int mq_close(struct mq *);

/* Write queue */
int mq_setmaxsize(struct mq *, size_t);
int mq_setmaxreloffset(struct mq *, size_t);
int mq_setmaxreltime(struct mq *, size_t);
size_t mq_getmaxsize(struct mq *);
size_t mq_getmaxreloffset(struct mq *);
size_t mq_getmaxreltime(struct mq *);

int mq_push(struct mq *, uint64_t, const char *, const void *, size_t);
int mq_rotate(struct mq *, uint64_t *);

/* Read queue */
int mq_info(struct mq *, struct mq_seginfo *);
int mq_next(struct mq *, struct mq_recinfo *);
int mq_payload(struct mq *, void *, size_t);
