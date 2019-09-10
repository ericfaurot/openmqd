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

#include "mqd.h"
#include "mq.h"

#include "io.h"
#include "log.h"
#include "proc.h"

#define IDLE_DELAY	120

#define	QUEUE_PERSIST	0x01

/*
 * XXX
 * - patterns should not take a reference on a queue
 */

struct observer {
	TAILQ_ENTRY(observer)	 tqe;
	void			(*fn)(void *, struct mqd_qevent *);
	void			*arg;
};

struct mqd_queue {
	SPLAY_ENTRY(mqd_queue)	 spe;
	TAILQ_ENTRY(mqd_queue)	 tqe;
	char			*name;
	int			 flags;
	int			 refcount;
	time_t			 idle;
	SIMPLEQ_HEAD(, msg)	 msgs;		/* pending messages */
	int			 error;
	struct mq		*mq;
	struct refs		 patterns;
};

static int queue_cmp(struct mqd_queue *, struct mqd_queue *);
static struct mqd_queue *queue_find(const char *);
static struct mqd_queue *queue_get(const char *);
static void queue_free(struct mqd_queue *);
static void queue_ref(struct mqd_queue *);
static void queue_unref(struct mqd_queue *);
static void queue_idle(struct mqd_queue *);
static void queue_idle_task(int, short, void *);
static struct msg *queue_new_msg(struct mqd_queue *, size_t, const char *);
static void queue_flush(struct mqd_queue *);
static void queue_event(struct mqd_qevent *);

struct __queue;

SPLAY_HEAD(__queue, mqd_queue);
SPLAY_PROTOTYPE(__queue, mqd_queue, spe, queue_cmp);

static int			qcount;
static struct __queue		queues;
static TAILQ_HEAD(, observer)	observers;
static TAILQ_HEAD(, mqd_queue)	idle;
static struct event		idle_evt;

void
queue_init(void)
{
	qcount = 0;
	SPLAY_INIT(&queues);
	TAILQ_INIT(&observers);
	TAILQ_INIT(&idle);
	evtimer_set(&idle_evt, queue_idle_task, NULL);
}

void
queue_shutdown(void)
{
	struct mqd_queue *q;

	SPLAY_FOREACH(q, __queue, &queues) {
		if (q->mq == NULL)
			continue;
		log_debug("closing queue %s", q->name);
		if (mq_close(q->mq) == -1)
			log_warn("mq_close: %s", q->name);
	}
}

void
queue_dispatch(struct imsgproc *proc, struct imsg *imsg)
{
	struct mqd_qevent evt;
	struct mqd_queue *q;
	const char *name;
	uint64_t offset;
	int mode;

	switch (imsg->hdr.type) {

	case IMSG_MQ_SCAN:
	case IMSG_MQ_OPEN:
		m_get_string(proc, &name);
		m_get_u64(proc, &offset);
		m_get_int(proc, &mode);
		m_end(proc);

		q = queue_find(name);
		if (q == NULL) {
			close(imsg->fd);
			return;
		}

		if (imsg->fd == -1)
			log_warnx("failed to receive fd");
		if ((q->mq = mq_fdopen(imsg->fd, offset, mode)) == NULL)
			log_warn("open queue %s", name);
		if (q->mq == NULL)
			q->error = 1;

		evt.event = (imsg->hdr.type == IMSG_MQ_SCAN) ?
		    QEV_OPEN : QEV_ROTATE;
		evt.timestamp = 0;
		evt.queue = q;
		evt.msg = NULL;
		queue_event(&evt);

		queue_flush(q);
		queue_unref(q); /* from queue_get()/queue_flush() */
		break;

	default:
		fatalx("%s: %s", __func__, log_fmt_imsgtype(imsg->hdr.type));
	}
}

/*
 * Validate a queue name.
 */
int
queue_validname(const char *name)
{
	if (name[0] == '#')
		name++;
	return validqname(name);
}

/*
 * Validate a message key.
 */
int
queue_validkey(const char *key)
{
	if (*key == '\0')
		return 0;
	return mq_validkey(key);
}

void
queue_info(struct io *io)
{
	struct mqd_queue *q;
	struct msg *msg;
	size_t n, m, t;

	n = 0;
	SPLAY_FOREACH(q, __queue, &queues) {
		io_printf(io, "queue.%zu.name=%s\n", n, q->name);
		io_printf(io, "queue.%zu.flags=0x%x\n", n, q->flags);
		io_printf(io, "queue.%zu.refcount=%d\n", n, q->refcount);
		io_printf(io, "queue.%zu.idle=%llu\n", n, q->idle);
		io_printf(io, "queue.%zu.error=%d\n", n, q->error);
		io_printf(io, "queue.%zu.patterns=%zu\n", n, ref_count(&q->patterns));
		m = 0;
		t = 0;
		SIMPLEQ_FOREACH(msg, &q->msgs, entry) {
			m++;
			t += msg->datalen;
		}
		io_printf(io, "queue.%zu.msg.count=%zu\n", n, m);
		io_printf(io, "queue.%zu.msg.size=%zu\n", n, t);
	}
	io_printf(io, "queue.count=%d\n", n);
}

const char *
queue_name(struct mqd_queue *q)
{
	return q->name;
}

int
queue_iter(void **hdl, struct mqd_queue **data)
{
	struct mqd_queue *curr = *hdl;

	if (curr == NULL)
		curr = SPLAY_MIN(__queue, &queues);
	else
		curr = SPLAY_NEXT(__queue, &queues, curr);

	if (curr) {
		*hdl = curr;
		if (data)
			*data = curr;
		return 1;
	}

	return 0;
}

/*
 * Prepare a message for this queue.
 */
struct msg *
queue_create_msg(const char *name, size_t len, const char *key)
{
	struct mqd_queue *q;
	struct msg *msg;

	q = queue_get(name);
	if (q == NULL)
		return NULL;
	msg = queue_new_msg(q, len, key);
	queue_unref(q); /* queue_get() */
	return msg;
}

/*
 * Push a message to its queue.
 */
void
queue_push_msg(struct msg *msg)
{
	struct mqd_qevent evt;

	mq_timestamp(&msg->timestamp);
	SIMPLEQ_INSERT_TAIL(&msg->queue->msgs, msg, entry);

	evt.event = QEV_MSG;
	evt.timestamp = msg->timestamp;
	evt.queue = msg->queue;
	evt.msg = msg;
	queue_event(&evt);

	queue_flush(msg->queue);
}

/*
 * Discard a new message without pushing it.
 */
void
queue_drop_msg(struct msg *msg)
{
	struct mqd_queue *q;

	q = msg->queue;
	free(msg->data);
	free(msg->key);
	free(msg);

	queue_unref(q); /* queue_new_msg() */
}

/*
 * Add a callback that will be triggered when a queue event occurs.
 */
int
queue_add_observer(void *arg, void (*fn)(void *, struct mqd_qevent *))
{
	struct observer *o;

	o = calloc(1, sizeof(*o));
	if (o == NULL)
		return -1;

	o->arg = arg;
	o->fn = fn;
	TAILQ_INSERT_TAIL(&observers, o, tqe);

	return 0;
}

/*
 * Remove a queue callback.
 */
void
queue_del_observer(void *arg)
{
	struct observer *o, *tmp;

	TAILQ_FOREACH_SAFE(o, &observers, tqe, tmp) {
		if (o->arg == arg) {
			TAILQ_REMOVE(&observers, o, tqe);
			free(o);
		}
	}
}

/*
 * Tell a queue that it is part of the given pattern.
 */
int
queue_attach_pattern(struct mqd_queue *q, struct mqd_pattern *p)
{
	return ref_add(&q->patterns, p);
}

/*
 * Remove the pattern from the set of pattern for this queue.
 */
int
queue_detach_pattern(struct mqd_queue *q, struct mqd_pattern *p)
{
	return ref_del(&q->patterns, p);
}

static int
queue_cmp(struct mqd_queue *a, struct mqd_queue *b)
{
	return strcmp(a->name, b->name);
}

SPLAY_GENERATE(__queue, mqd_queue, spe, queue_cmp);

static struct mqd_queue *
queue_find(const char *name)
{
	struct mqd_queue *q, key;

	key.name = (char*)name;
	q = SPLAY_FIND(__queue, &queues, &key);

	return q;
}

static struct mqd_queue *
queue_get(const char *name)
{
	struct mqd_qevent evt;
	struct mqd_queue *q;

	q = queue_find(name);
	if (q == NULL) {
		q = calloc(1, sizeof(*q));
		if (q == NULL)
			return NULL;
		q->name = strdup(name);
		if (q->name == NULL) {
			free(q);
			return NULL;
		}

		ref_init(&q->patterns);
		SIMPLEQ_INIT(&q->msgs);
		SPLAY_INSERT(__queue, &queues, q);
		if (name[0] != '#')
			q->flags |= QUEUE_PERSIST;
		qcount++;

		if (q->flags & QUEUE_PERSIST) {
			m_create(p_engine, IMSG_MQ_SCAN, 0, 0, -1);
			m_add_string(p_engine, name);
			m_close(p_engine);
			queue_ref(q);
		}

		evt.timestamp = 0;
		evt.event = QEV_NEW;
		evt.queue = q;
		evt.msg = NULL;
		queue_event(&evt);
	}
	if (q)
		queue_ref(q);

	return q;
}

static void
queue_free(struct mqd_queue *q)
{
	struct mqd_qevent evt;

	evt.event = QEV_FREE;
	evt.timestamp = 0;
	evt.queue = q;
	evt.msg = NULL;
	queue_event(&evt);

	/*
	 * Discard pattern references. The patterns have already dropped
	 * the back-reference when processing the qevent.
	 */
	while (ref_pop(&q->patterns))
		;

	SPLAY_REMOVE(__queue, &queues, q);
	if (q->mq)
		if (mq_close(q->mq) == -1)
			log_warn("mq_close: %s", q->name);
	free(q->name);
	free(q);
	qcount--;
}

static void
queue_ref(struct mqd_queue *q)
{
	int first;

	if (q->refcount++ == 0 && q->idle) {
		first = TAILQ_FIRST(&idle) == q;
		TAILQ_REMOVE(&idle, q, tqe);
		q->idle = 0;
		if (first) {
			evtimer_del(&idle_evt);
			queue_idle_task(-1, 0, NULL);
		}
	}
}

static void
queue_unref(struct mqd_queue *q)
{
	if (--q->refcount)
		return;

	queue_idle(q);
}

static void
queue_idle(struct mqd_queue *q)
{
	struct mqd_qevent evt;
	struct timeval tv;

	time(&q->idle);
	TAILQ_INSERT_TAIL(&idle, q, tqe);
	if (TAILQ_FIRST(&idle) == q) {
		tv.tv_sec = IDLE_DELAY;
		tv.tv_usec = 0;
		evtimer_add(&idle_evt, &tv);
	}

	evt.event = QEV_IDLE;
	evt.timestamp = 0;
	evt.queue = q;
	evt.msg = NULL;
	queue_event(&evt);
}

static void
queue_idle_task(int fd, short ev, void *arg)
{
	struct mqd_queue *q;
	struct timeval tv;
	time_t	t;

	time(&t);

	while ((q = TAILQ_FIRST(&idle))) {
		if (t < q->idle + IDLE_DELAY) {
			tv.tv_sec = IDLE_DELAY - (t - q->idle);
			tv.tv_usec = 0;
			evtimer_add(&idle_evt, &tv);
			return;
		}
		TAILQ_REMOVE(&idle, q, tqe);
		queue_free(q);
	}
}

static struct msg *
queue_new_msg(struct mqd_queue *q, size_t len, const char *key)
{
	struct msg *msg;

	msg = calloc(1, sizeof(*msg));
	if (msg == NULL)
		return NULL;

	if ((msg->data = malloc(len)) == NULL)
		goto fail;

	if (key && (msg->key = strdup(key)) == NULL)
		goto fail;

	msg->datalen = len;
	msg->queue = q;
	queue_ref(q);
	return msg;

    fail:
	free(msg->key);
	free(msg->data);
	free(msg);
	return NULL;
}

static void
queue_flush(struct mqd_queue *q)
{
	uint64_t offset;
	struct msg *msg;
	int r;

	if (q->flags & QUEUE_PERSIST && q->mq == NULL && !q->error)
		return;

	/*
	 * Take a reference to make sure the queue doesn't get invalidated
	 * within the loop when the last message is processed.
	 */
	queue_ref(q);

	while ((msg = SIMPLEQ_FIRST(&q->msgs))) {

		if (q->flags & QUEUE_PERSIST && !q->error) {
			r = mq_push(q->mq, msg->timestamp, msg->key,
			    msg->data, msg->datalen);
			if (r == -1) {
				log_warn("mq_push");
				if (mq_close(q->mq) == -1)
					log_warn("mq_close: %s", q->name);
				q->mq = NULL;
				q->error = 1;
			}
			else if (r == 0) {
				/*
				 * Must rotate the segment file.
				 */
				mq_rotate(q->mq, &offset);
				q->mq = NULL;
				m_create(p_engine, IMSG_MQ_OPEN, 0, 0, -1);
				m_add_string(p_engine, q->name);
				m_add_u64(p_engine, offset);
				m_add_int(p_engine, 1);
				m_close(p_engine);
				queue_ref(q);
				break;
			}
		}

		if (q->flags & QUEUE_PERSIST && q->error)
			log_warnx("lost message on queue %s", q->name);

		SIMPLEQ_REMOVE_HEAD(&q->msgs, entry);
		free(msg->key);
		free(msg->data);
		free(msg);
		queue_unref(q); /* queue_new_msg() */
	}

	queue_unref(q); /* See above. */
}

/*
 * Broadcast a queue event.
 */
static void
queue_event(struct mqd_qevent *evt)
{
	static int nonce = 0;
	struct mqd_pattern *p;
	struct observer *o, *tmp;
	void *iter;

	evt->nonce = nonce++;
	evt->qname = evt->queue->name;
	if (evt->timestamp == 0)
		mq_timestamp(&evt->timestamp);

	/* Broadcast to global observers in priority. */
	TAILQ_FOREACH_SAFE(o, &observers, tqe, tmp)
		o->fn(o->arg, evt);

	/* Broadcast patterns matching this queue. */
	iter = NULL;
	while ((p = ref_iter(&evt->queue->patterns, &iter)))
		pattern_qevent(p, evt);

	/* If some client need to be finalized. */
	mqd_client_flush();
}
