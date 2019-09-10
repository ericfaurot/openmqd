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

#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "shbuf.h"

struct shbuf *
shbuf_malloc(size_t len)
{
	struct shbuf *buf;

	if (len >= SIZE_MAX - sizeof(*buf)) {
		errno = ENOMEM;
		return NULL;
	}

	buf = malloc(sizeof(buf) + len);
	if (buf) {
		buf->buf = (char*)(buf + 1);
		buf->len = 0;
		buf->refcount = 1;
	}
	return buf;
}

struct shbuf *
shbuf_strdup(const char *str)
{
	return shbuf_memdup(str, strlen(str) + 1);
}

struct shbuf *
shbuf_memdup(const void *src, size_t len)
{
	struct shbuf *buf;

	if ((buf = shbuf_malloc(len)))
		memmove(buf->buf, src, len);
	return buf;
}

void
shbuf_free(struct shbuf *buf)
{
	if (buf && --buf->refcount == 0)
		free(buf);
}

void
shbuf_ref(struct shbuf *buf)
{
	buf->refcount++;
}
