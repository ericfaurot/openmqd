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
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "mq.h"

#define REC_MAXSIZE	(1 << 25)	/* 32M */

#define REC_TYPEMASK	0x000000ff
#define REC_HASKEY	0x00000100 /* record has a key */

#define SEG_VERSION	1

#define DEFAULT_MAXSIZE		(1 << 30)	/* 1G */
#define DEFAULT_MAXRELOFFSET	(1 << 24)	/* 16M */
#define DEFAULT_MAXRELTIME	(1 << 30)	/* 12j10h15m41s.824 */

struct mqseghdr {
	char		magic[4];
	uint32_t	version;
	uint32_t	flags;
	uint32_t	_pad;
	uint64_t	baseoffset;
	uint64_t	basetime;
} __packed;

struct mqrechdr {
	uint8_t		type;
	uint8_t		count;
	uint16_t	flags;
	uint32_t	reloffset;
	uint32_t	reltime;
	uint32_t	size;
} __packed;

struct mq {
	int		fd;		/* segment file */
	int		mode;		/* open for writing */

	uint32_t	maxsize;	/* maximum file size */
	uint32_t	maxreloffset;	/* maximum number of records */
	uint32_t	maxreltime;	/* maximum relative time */

	/* Queue metadata */
	int		version;
	int		flags;
	uint64_t	baseoffset;
	uint64_t	basetime;
	uint64_t	currtime;	/* (w) timestamp of the last message */
	uint32_t	currreloffset;	/* (w) offset for the next message */

	int		dirty;		/* (w) some messages have been written */
	int		corrupt;	/* (w) file had write error */
	int		end;		/* (w) end is reached */
	off_t		pos;		/* (w) current file length */

	size_t		mlen;		/* (r) next message size */
};

static int mq_free(struct mq *, int);
static int mq_pushrec(struct mq *, int, uint64_t, const char *,
    const struct iovec *, int);

static void mq_timeval(uint64_t, struct timeval *);
static int mq_readseghdr(int, struct mqseghdr *);
static int mq_readrechdr(int, struct mqrechdr *);
static int mq_writev(int, struct iovec *, int, size_t *);
static int mq_read(int, void *, size_t);

/*
 * Get the current time in milliseconds.
 */
int
mq_timestamp(uint64_t *t)
{
	struct timespec tp;
	if (clock_gettime(CLOCK_REALTIME, &tp) == -1) {
		*t = 0;
		return -1;
	}

	*t = (tp.tv_sec * 1000) + tp.tv_nsec / 1000000;
	return 0;
};

int
mq_validkey(const char *key)
{
	int i, c;

	for (i = 0; i < MSG_MAXKEYLEN; i++) {
		c  = (unsigned int)key[i];
		if (c == 0)
			return 1;
		if (c <= 0x20 || c >= 0x7f) {
			errno = EINVAL;
			return 0;
		}
	}

	errno = EMSGSIZE;
	return 0;
}

/*
 * Convert a timestamp to a timeval.
 */
static void
mq_timeval(uint64_t ts, struct timeval *tv)
{
	tv->tv_sec = ts / 1000;
	tv->tv_usec = (ts % 1000) * 1000;
}

/*
 * Find the highest segmentfile in the given directory.
 * Set the offset and return 1 if found, 0 if not found, -1 on error.
 */
int
mq_scan(const char *path, uint64_t *offset)
{
	struct dirent *dp;
	DIR *dirp;
	uint64_t o;
	int found = 0, n;

	dirp = opendir(path);
	if (dirp == NULL) {
		return -1;
	}

	*offset = 0;
	while ((dp = readdir(dirp)) != NULL) {
		n = 0;
		if (dp->d_type != DT_REG ||
		    dp->d_namlen != 20 ||
		    strcmp(dp->d_name + 16, ".log") ||
		    sscanf(dp->d_name, "%016llx.log%n", &o, &n) != 1 ||
		    n != 20)
				continue;
		if (!found || o > *offset) {
			*offset = o;
			found = 1;
		}
	}
	closedir(dirp);

	return found;
}

/*
 * Open the segment file for reading or writing starting at the given offset.
 */
int
mq_open(const char *dirname, uint64_t offset, int mode)
{
	char path[PATH_MAX];
	int fd;

	snprintf(path, sizeof(path), "%s/%016llx.log", dirname, offset);
	if (mode == MQ_WRITE) {
		fd = open(path, O_RDWR|O_CREAT|O_EXCL|O_APPEND, 0644);
		if (fd == -1 && (errno == EEXIST))
			fd = open(path, O_RDWR|O_APPEND);
	}
	else if (mode == MQ_READ)
		fd = open(path, O_RDONLY);
	else {
		errno = EINVAL;
		return -1;
	}

	return fd;
}

/*
 * Unlink the segment file for the given offset.
 */
int
mq_unlink(const char *dirname, uint64_t offset)
{
	char path[PATH_MAX];

	snprintf(path, sizeof(path), "%s/%016llx.log", dirname, offset);
	return unlink(path);
}


/*
 * Use the file description as the current segment for the queue.
 * Return -1 on error, 0 otherwise.
 */
struct mq *
mq_fdopen(int fd, uint64_t offset, int mode)
{
	struct mqseghdr seghdr;
	struct mqrechdr rechdr;
	struct stat st;
	int saved_errno;
	struct mq *mq;

	if (mode != MQ_READ && mode != MQ_WRITE) {
		errno = EINVAL;
		return NULL;
	}

	if ((mq = calloc(1, sizeof(*mq))) == NULL)
		return NULL;

	mq->fd = fd;
	mq->mode = mode;
	mq->maxsize = DEFAULT_MAXSIZE;
	mq->maxreloffset = DEFAULT_MAXRELOFFSET;
	mq->maxreltime = DEFAULT_MAXRELTIME;

	if (fstat(fd, &st) == -1)
		goto fail;

	/* New file, nothing to be done. */
	if (st.st_size == 0) {
		mq->baseoffset = offset;
		return mq;
	}

	/* Read the segment header. */
	if (mq_readseghdr(fd, &seghdr) == -1)
		goto fail;

	/* Validate the header signature. */
	if (seghdr.magic[0] != 'O' ||
	    seghdr.magic[1] != 'M' ||
	    seghdr.magic[2] != 'Q' ||
	    seghdr.magic[3] != 'D' ||
	    seghdr.version != SEG_VERSION ||
	    seghdr._pad != 0) {
		errno = EFTYPE;
		goto fail;
	}

	/* If an offset is specified */
	if (seghdr.baseoffset != offset && (mode == MQ_WRITE || offset)) {
		errno = EFTYPE;
		goto fail;
	}
	mq->baseoffset = seghdr.baseoffset;
	mq->basetime = seghdr.basetime;
	mq->version = seghdr.version;
	mq->flags = seghdr.flags;

	/* When writing to the file, go read the checkpoint record. */
	if (mode == MQ_WRITE) {
		if (lseek(mq->fd, -sizeof(rechdr), SEEK_END) == -1)
			goto fail;
		if (mq_readrechdr(fd, &rechdr) == -1)
			goto fail;
		if (rechdr.type != REC_CHECKPOINT &&
		    rechdr.type != REC_ROTATE) {
			errno = EFTYPE;
			goto fail;
		}
		if (rechdr.size != 0) {
			errno = EFTYPE;
			goto fail;
		}
		mq->currtime = mq->basetime + rechdr.reltime;
		mq->currreloffset = rechdr.reloffset;
		mq->pos = st.st_size;
		if (rechdr.type == REC_ROTATE)
			mq->end = 1;
	}

	return mq;

    fail:
	saved_errno = errno;
	free(mq);
	errno = saved_errno;
	return NULL;
}

/*
 * Create a queue for the given segment.
 */
struct mq *
mq_diropen(const char *dirname, uint64_t offset, int mode)
{
	struct mq *mq;
	int fd, saved_errno;

	if ((fd = mq_open(dirname, offset, mode)) == -1)
		return NULL;
	if ((mq = mq_fdopen(fd, offset, mode)) == NULL) {
		saved_errno = errno;
		close(fd);
		errno = saved_errno;
	}

	return mq;
}

/*
 * Close the current segment file.
 */
int
mq_close(struct mq *mq)
{
	return mq_free(mq, 0);
}

int
mq_setmaxsize(struct mq *mq, size_t v)
{
	if (v > DEFAULT_MAXSIZE) {
		errno = ERANGE;
		return -1;
	}

	mq->maxsize = v;
	return 0;
}

int
mq_setmaxreloffset(struct mq *mq, size_t v)
{
	if (v > DEFAULT_MAXRELOFFSET) {
		errno = ERANGE;
		return -1;
	}

	mq->maxreloffset = v;
	return 0;
}

int
mq_setmaxreltime(struct mq *mq, size_t v)
{
	if (v > DEFAULT_MAXRELTIME) {
		errno = ERANGE;
		return -1;
	}

	mq->maxreltime = v;
	return 0;
}

size_t
mq_getmaxsize(struct mq *mq)
{
	if (mq)
		return mq->maxsize;
	return DEFAULT_MAXSIZE;
}

size_t
mq_getmaxreloffset(struct mq *mq)
{
	if (mq)
		return mq->maxreloffset;
	return DEFAULT_MAXRELOFFSET;
}

size_t
mq_getmaxreltime(struct mq *mq)
{
	if (mq)
		return mq->maxreltime;
	return DEFAULT_MAXRELTIME;
}

/*
 * Close the current segment and report the next offset.
 */
int
mq_rotate(struct mq *mq, uint64_t *poffset)
{
	if (mq->mode != MQ_WRITE) {
		errno = EBADF;
		return -1;
	}
	*poffset = mq->baseoffset + mq->currreloffset;
	return mq_free(mq, 1);
}

/*
 * Get info from the current segment.
 */
int
mq_info(struct mq *mq, struct mq_seginfo *nfo)
{
	nfo->version = mq->version;
	nfo->flags = mq->flags;
	nfo->baseoffset = mq->baseoffset;
	nfo->curroffset = mq->baseoffset + mq->currreloffset;
	mq_timeval(mq->basetime, &nfo->basetime);
	mq_timeval(mq->currtime, &nfo->currtime);

	return 0;
}

/*
 * Push a new message on this queue.
 * Return -1 on error, 0 if the queue needs to be rotated, and 1 on success.
 */
int
mq_push(struct mq *mq, uint64_t timestamp, const char *key,
    size_t datalen, const void *data)
{
	struct iovec iov;
	size_t keylen;

	if (mq->mode != MQ_WRITE) {
		errno = EBADF;
		return -1;
	}

	/* Make sure the parameters are correct. */
	keylen = key ? strlen(key) : 0;
	if (datalen > MSG_MAXDATALEN || keylen > MSG_MAXKEYLEN) {
		errno = EMSGSIZE;
		return -1;
	}

	iov.iov_base = (void*)data;
	iov.iov_len = datalen;
	return mq_pushrec(mq, REC_MESSAGE, timestamp, key, &iov,
	    (data && datalen) ? 1 : 0);
}

/*
 * Read the next message and fill the mq_recinfo structure.
 * Skip the current content if it was not read by the caller.
 */
int
mq_next(struct mq *mq, struct mq_recinfo *nfo)
{
	struct mqrechdr rechdr;

	if (mq->mode != MQ_READ) {
		errno = EBADF;
		return -1;
	}

	if (mq->mlen) {
		/* XXX the data might not be there yet */
		if (lseek(mq->fd, mq->mlen, SEEK_CUR) == -1)
			return -1;
	}

	if (mq_readrechdr(mq->fd, &rechdr) == -1)
		return -1;

	mq->mlen = rechdr.size;

	nfo->type = rechdr.type;
	nfo->key = (rechdr.flags & REC_HASKEY) ? 1 : 0;
	nfo->size = rechdr.size;
	nfo->offset = mq->baseoffset + rechdr.reloffset;
	mq_timeval(mq->basetime + rechdr.reltime, &nfo->timestamp);

	return 0;
}

/*
 * Retreive the current message payload.
 */
int
mq_payload(struct mq *mq, void *buf, size_t len)
{
	int r;

	if (mq->mode != MQ_READ) {
		errno = EBADF;
		return -1;
	}

	if (len > mq->mlen)
		len = mq->mlen;

	if (buf == NULL)
		/* XXX the data might not be there yet */
		r = lseek(mq->fd, len, SEEK_CUR);
	else
		r = mq_read(mq->fd, buf, len);

	if (r == -1)
		return -1;

	mq->mlen -=  len;
	return 0;
}

/*
 * Release all segment resources, and reset the structure.
 */
static int
mq_free(struct mq *mq, int rotate)
{
	struct iovec iov;
	struct mqrechdr rechdr;
	size_t written;
	int r = 0, saved_errno;

	if (mq->dirty) {
		/* Write a checkpoint or rotate control message.*/
		memset(&rechdr, 0, sizeof(rechdr));
		rechdr.type = rotate ? REC_ROTATE : REC_CHECKPOINT;
		rechdr.reltime = htobe32(mq->currtime - mq->basetime);
		rechdr.reloffset = htobe32(mq->currreloffset);
		iov.iov_base = &rechdr;
		iov.iov_len = sizeof(rechdr);
		r = mq_writev(mq->fd, &iov, 1, &written);
	}
	
	if (rotate)
		fchmod(mq->fd, S_IRUSR | S_IRGRP | S_IROTH);

	if (r == -1)
		saved_errno = errno;
	close(mq->fd);
	free(mq);
	if (r == -1)
		errno = saved_errno;
	return r;
}

static int
mq_pushrec(struct mq *mq, int type, uint64_t timestamp, const char *key,
    const struct iovec *iop, int ioc)
{
	struct mqseghdr seghdr;
	struct mqrechdr rechdr;
	struct iovec iov[IOV_MAX];
	uint64_t basetime;
	size_t size = 0, w;
	int saved_errno, i = 0;

	/* Check if the segment is not too big already. */
	if (mq->end ||
	    mq->currreloffset >= mq->maxreloffset ||
	    mq->pos >= mq->maxsize)
		return 0;

	if (timestamp == 0 && mq_timestamp(&timestamp) == -1)
		return -1;

	basetime = mq->basetime;
	if (basetime == 0) {
		/*
		 * If the base time is not decided yet, it is set to the
		 * timestamp of the first message.
		 */
		basetime = timestamp;
		rechdr.reltime = 0;
	}
	else {
		/* Fix timestamp if necessary. */
		if (timestamp < mq->currtime)
			timestamp = mq->currtime;

		/* Check if the timerange for the segment is not too large. */
		if (timestamp - basetime > mq->maxreltime)
			return 0;
		rechdr.reltime = htobe32(timestamp - basetime);
	}

	/* Prepend the segment file header if necessary. */
	if (mq->pos == 0) {
		seghdr.magic[0] = 'O';
		seghdr.magic[1] = 'M';
		seghdr.magic[2] = 'Q';
		seghdr.magic[3] = 'D';
		seghdr.version = htobe32(SEG_VERSION);
		seghdr.flags = 0;
		seghdr._pad = 0;
		seghdr.baseoffset = htobe64(mq->baseoffset);
		seghdr.basetime = htobe64(basetime);
		iov[i].iov_base = &seghdr;
		iov[i].iov_len = sizeof(seghdr);
		i++;
	}

	/* Add the message header. */
	iov[i].iov_base = &rechdr;
	iov[i].iov_len = sizeof(rechdr);
	i++;

	/* Add the message key if there is one. */
	if (key) {
		iov[i].iov_base = (void*)key;
		iov[i].iov_len = strlen(key) + 1;
		size += iov[i].iov_len;
		if (size > REC_MAXSIZE) {
			errno = EMSGSIZE;
			return -1;
		}
		i++;
	}

	/* Add the payload. */
	while (ioc) {
		if (iop->iov_len > REC_MAXSIZE) {
			errno = EMSGSIZE;
			return -1;
		}
		iov[i].iov_base = iop->iov_base;
		iov[i].iov_len = iop->iov_len;
		size += iov[i].iov_len;
		if (size > REC_MAXSIZE) {
			errno = EMSGSIZE;
			return -1;
		}
		ioc--;
		iop++;
		i++;
	}

	rechdr.type = type;
	rechdr.count = 0;
	rechdr.flags = htobe16(key ? REC_HASKEY : 0);
	rechdr.reloffset = htobe32(mq->currreloffset);
	rechdr.size = htobe32(size);

	if (mq_writev(mq->fd, iov, i, &w) == -1) {
		/*  Reset the file length if needed. */
		if (w) {
			saved_errno = errno;
			if (ftruncate(mq->fd, mq->pos) == -1)
				mq->corrupt = 1;
			errno = saved_errno;
		}
		return -1;
	}

	if (mq->basetime == 0)
		mq->basetime = timestamp;
	mq->currtime = timestamp;
	mq->currreloffset++;
	mq->pos += w;
	mq->dirty += 1;

	return 1;
}

/*
 * Read a segment header from a fd.
 */
static int
mq_readseghdr(int fd, struct mqseghdr *seghdr)
{
	if (mq_read(fd, seghdr, sizeof(*seghdr)) == -1)
		return -1;

	seghdr->version = betoh32(seghdr->version);
	seghdr->flags = betoh32(seghdr->flags);
	seghdr->_pad = betoh32(seghdr->flags);
	seghdr->baseoffset = betoh64(seghdr->baseoffset);
	seghdr->basetime = betoh64(seghdr->basetime);

	return 0;
}


/*
 * Read a message header from a fd.
 */
static int
mq_readrechdr(int fd, struct mqrechdr *rechdr)
{
	if (mq_read(fd, rechdr, sizeof(*rechdr)) == -1)
		return -1;

	rechdr->flags = betoh16(rechdr->flags);
	rechdr->reloffset = betoh32(rechdr->reloffset);
	rechdr->reltime = betoh32(rechdr->reltime);
	rechdr->size = betoh32(rechdr->size);

	return 0;
}

/*
 * Read data from a fd.
 */
static int
mq_read(int fd, void *dst, size_t dstsz)
{
	ssize_t r;
	size_t len;
	char *data;

	data = (char*)dst;
	len = dstsz;
	while (len) {
		r = read(fd, data, len);
		if (r == -1) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		if (r == 0) {
			errno = EAGAIN;
			return -1;
		}
		len -= r;
		data += r;
	}
	return 0;
}

static int
mq_writev(int fd, struct iovec *iov, int ioc, size_t *wp)
{
	ssize_t w;

	*wp = 0;

	while (ioc) {
		w = writev(fd, iov, ioc);
		if (w == -1) {
			if (errno == EINTR)
				continue;
			return -1;
		}

		*wp += w;
		while (ioc && iov->iov_len <= (size_t)w) {
			w -= iov->iov_len;
			iov++;
			ioc--;
		}
		if (ioc) {
			iov->iov_len -= w;
			iov->iov_base = (char *)(iov->iov_base) + w;
		}
	}

	return 0;
}
