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

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fnmatch.h>

#include "mqd.h"
#include "mq.h"

#include "io.h"
#include "log.h"

struct mqd_pattern {
	SPLAY_ENTRY(mqd_pattern) spe;
	char			*pattern;
	int			 type;
	int			 refcount;
	struct refs		 clients;	/* subscribed clients */
	struct refs		 queues;	/* matching queues */
};

static int pattern_cmp(struct mqd_pattern *, struct mqd_pattern *);
static struct mqd_pattern *pattern_find(const char *, int);
static void pattern_free(struct mqd_pattern *);
static int pattern_check_queue(struct mqd_pattern *, struct mqd_queue *);
static int pattern_match(struct mqd_pattern *, const char *);
static void pattern_observer_cb(void *, struct mqd_qevent *);

struct __pattern;

SPLAY_HEAD(__pattern, mqd_pattern);
SPLAY_PROTOTYPE(__pattern, mqd_pattern, spe, pattern_cmp);

static struct __pattern patterns;

void
pattern_init(void)
{
	SPLAY_INIT(&patterns);
}

void
pattern_shutdown(void)
{
}

void
pattern_info(struct io *io)
{
	size_t n;
	struct mqd_pattern *p;

	n = 0;
	SPLAY_FOREACH(p, __pattern, &patterns) {
		io_printf(io, "pattern.%zu.type=%d\n", n, p->type);
		if (p->pattern)
			io_printf(io, "pattern.%zu.name=%s\n", n, p->pattern);
		io_printf(io, "pattern.%zu.clients=%zu\n", n, ref_count(&p->clients));
		io_printf(io, "pattern.%zu.queues=%zu\n", n, ref_count(&p->queues));
		n++;
	}
	io_printf(io, "pattern.count=%d\n", n);
}

void
pattern_ref(struct mqd_pattern *p)
{
	p->refcount++;
}

void
pattern_unref(struct mqd_pattern *p)
{
	if (--p->refcount == 0)
		pattern_free(p);
}

int
pattern_attach_client(struct mqd_pattern *p, struct mqd_client *clt)
{
	int r;

	r = ref_add(&p->clients, clt);
	if (r == 1)
		pattern_ref(p);

	return r;
}

int
pattern_detach_client(struct mqd_pattern *p, struct mqd_client *clt)
{
	int r;

	r = ref_del(&p->clients, clt);
	if (r == 1)
		pattern_unref(p);

	return r;
}

void
pattern_qevent(struct mqd_pattern *p, struct mqd_qevent *evt)
{
	struct mqd_client *clt;
	void *iter;

	if (evt->event == QEV_MSG) {
		/*
		 * Broadcast message to all clients listening on that pattern.
		 * Clients must not be removed during the iteration.
		 */
		iter = NULL;
		while ((clt = ref_iter(&p->clients, &iter)))
			mqd_client_qevent(clt, evt);
	}
	else if (evt->event == QEV_FREE)
		/*
		 * Unlink the queue from this pattern.
		 */
		ref_del(&p->queues, evt->queue);
}

static int
pattern_cmp(struct mqd_pattern *a, struct mqd_pattern *b)
{
	if (a->type < b->type)
		return -1;
	if (a->type > b->type)
		return 1;
	if (a->type == PATTERN_ALL)
		return 0;
	return strcmp(a->pattern, b->pattern);
}

SPLAY_GENERATE(__pattern, mqd_pattern, spe, pattern_cmp);

static struct mqd_pattern *
pattern_find(const char *pattern, int type)
{
	struct mqd_pattern *p, key;

	key.pattern = (char *)pattern;
	key.type = type;
	p = SPLAY_FIND(__pattern, &patterns, &key);

	return p;
}

struct mqd_pattern *
pattern_get(const char *pattern, int type)
{
	static int init = 0;
	struct mqd_pattern *p;
	struct mqd_queue *q;
	void *iter;

	p = pattern_find(pattern, type);
	if (p == NULL) {
		if (init == 0) {
			queue_add_observer(&patterns, pattern_observer_cb);
			init = 1;
		}
		p = calloc(1, sizeof(*p));
		if (p == NULL)
			return NULL;
		if (pattern) {
			p->pattern = strdup(pattern);
			if (p->pattern == NULL) {
				free(p);
				return NULL;
			}
		}
		ref_init(&p->clients);
		ref_init(&p->queues);
		p->type = type;
		SPLAY_INSERT(__pattern, &patterns, p);

		/* Iterate queues to find those that match. */
		iter = NULL;
		while (queue_iter(&iter, &q))
			/* XXX error checking */
			pattern_check_queue(p, q);
	}
	if (p)
		pattern_ref(p);

	return p;
}

static void
pattern_free(struct mqd_pattern *p)
{
	struct mqd_queue *q;

	while ((q = ref_pop(&p->queues)))
		queue_detach_pattern(q, p);

	SPLAY_REMOVE(__pattern, &patterns, p);
	free(p->pattern);
	free(p);
}

static int
pattern_check_queue(struct mqd_pattern *p, struct mqd_queue *q)
{
	int r;

	if (!pattern_match(p, queue_name(q)))
		return 0;

	r = queue_attach_pattern(q, p);
	if (r == 1) {
		r = ref_add(&p->queues, q);
		if (r == -1)
			queue_detach_pattern(q, p);
	}
	return r;
}

static int
pattern_match(struct mqd_pattern *p, const char *name)
{
	switch (p->type) {
	case PATTERN_ALL:
		return 1;
	case PATTERN_EXACT:
		return (strcmp(p->pattern, name) == 0);
	case PATTERN_FNMATCH:
		return (fnmatch(p->pattern, name, 0) == 0);
	default:
		return 0;
	}
}

static void
pattern_observer_cb(void *arg, struct mqd_qevent *evt)
{
	struct mqd_pattern *p;

	if (evt->event == QEV_NEW)
		SPLAY_FOREACH(p, __pattern, &patterns)
			/* XXX error checking */
			pattern_check_queue(p, evt->queue);
}
