/*	$OpenBSD$	*/
/*
 * Copyright (c) 2012 Eric Faurot <eric@openbsd.org>
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
#include <sys/tree.h>

#include <stdlib.h>

#include "mqd.h"

struct refentry {
	SPLAY_ENTRY(refentry)	 entry;
	void			*ptr;
};

static int refentry_cmp(struct refentry *, struct refentry *);

SPLAY_PROTOTYPE(refs, refentry, entry, refentry_cmp);

int
ref_add(struct refs *t, const void *ptr)
{
	struct refentry	*entry;

	if ((entry = malloc(sizeof *entry)) == NULL)
		return -1;

	entry->ptr = (void*)ptr;
	if (SPLAY_INSERT(refs, t, entry) != NULL) {
		free(entry);
		return 0;
	}

	return 1;
}

int
ref_del(struct refs *t, const void *ptr)
{
	struct refentry	key, *entry;

	key.ptr = (void*)ptr;
	if ((entry = SPLAY_FIND(refs, t, &key)) == NULL)
		return 0;

	SPLAY_REMOVE(refs, t, entry);
	free(entry);

	return 1;
}

void *
ref_pop(struct refs *t)
{
	struct refentry	*entry;
	void *r;

	entry = SPLAY_ROOT(t);
	if (entry == NULL)
		return NULL;

	r = entry->ptr;
	SPLAY_REMOVE(refs, t, entry);
	free(entry);
	return r;
}

void *
ref_iter(struct refs *t, void **hdl)
{
	struct refentry *curr = *hdl;

	if (curr == NULL)
		curr = SPLAY_MIN(refs, t);
	else
		curr = SPLAY_NEXT(refs, t, curr);

	if (curr) {
		*hdl = curr;
		return curr->ptr;
	}

	return NULL;
}

size_t
ref_count(struct refs *t)
{
	struct refentry *entry;
	size_t s;

	s = 0;
	SPLAY_FOREACH(entry, refs, t)
		s++;
	return s;
}

static int
refentry_cmp(struct refentry *a, struct refentry *b)
{
	if (a->ptr < b->ptr)
		return -1;
	if (a->ptr > b->ptr)
		return 1;
	return 0;
}

SPLAY_GENERATE(refs, refentry, entry, refentry_cmp);
