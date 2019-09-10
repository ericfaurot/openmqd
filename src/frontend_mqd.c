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

enum {
	STATE_READ_COMMAND,
	STATE_READ_DATA,
	STATE_MONITOR,
	STATE_LISTEN,
	STATE_QUIT
};

struct mqd_client {
	TAILQ_ENTRY(mqd_client)	 tqe;
	TAILQ_ENTRY(mqd_client)	 tqe2;
	uint32_t		 id;
	struct io		*io;
	int			 state;
	char			*pos;		/* where to write	*/
	size_t			 expect;	/* how much to write	*/
	struct msg		*msg;		/* current message	*/

	struct refs		 patterns;
	int			 lastnonce;
	uint64_t		 lasttimestamp;
};

static void mqd_client_info(struct io *);
static void mqd_dispatch_io(struct io *, int, void *);
static void mqd_input(struct mqd_client *);
static void mqd_command(struct mqd_client *, char *);
static void mqd_close(struct mqd_client *);
static void mqd_quit(struct mqd_client *, const char *);
static void mqd_respond(struct mqd_client *, const char *);
static void mqd_cmd_info(struct mqd_client*, int, char **);
static void mqd_cmd_listen(struct mqd_client*, int, char **);
static void mqd_cmd_monitor(struct mqd_client*, int, char **);
static void mqd_cmd_push(struct mqd_client*, int, char **);
static void mqd_cmd_push_done(struct mqd_client*);
static void mqd_cmd_quit(struct mqd_client*, int, char **);
static void mqd_cmd_subscribe(struct mqd_client*, int, char **);
static void mqd_monitor_observer_cb(void *, struct mqd_qevent *);

static TAILQ_HEAD(, mqd_client)	clients;
static TAILQ_HEAD(, mqd_client)	monitors;
static TAILQ_HEAD(, mqd_client)	zombies;

void
frontend_mqd_init(void)
{
	TAILQ_INIT(&clients);
	TAILQ_INIT(&monitors);
	TAILQ_INIT(&zombies);
}

void
frontend_mqd_shutdown(void)
{
}

void
frontend_mqd_conn(uint32_t connid, struct listener *l, int sock,
    const struct sockaddr *sa)
{
	struct mqd_client *clt;

	if ((clt = calloc(1, sizeof(*clt))) == NULL) {
		log_warn("%s: calloc", __func__);
		close(sock);
		frontend_conn_closed(connid);
		return;
	}

	ref_init(&clt->patterns);
	clt->id = connid;
	clt->io = io_new();
	if (clt->io == NULL) {
		free(clt);
		close(sock);
		frontend_conn_closed(connid);
		return;
	}
	clt->state = STATE_READ_COMMAND;
	io_set_callback(clt->io, mqd_dispatch_io, clt);
	io_set_read(clt->io);
	io_set_timeout(clt->io, 300000);
	io_attach(clt->io, sock);
	TAILQ_INSERT_TAIL(&clients, clt, tqe);
}

void
frontend_mqd_dispatch(struct imsgproc *proc, struct imsg *imsg)
{
	switch (imsg->hdr.type) {
	default:
		fatalx("%s: %s", __func__, log_fmt_imsgtype(imsg->hdr.type));
	}
}

/*
 * Notify a client that a qevent has occured.
 */
void
mqd_client_qevent(struct mqd_client *clt, struct mqd_qevent *evt)
{
	int r;

	/*
	 * Only forward event when after the LISTEN command has been issued.
	 */
	if (clt->state != STATE_LISTEN)
		return;

	/*
	 * Make sure the same event is not processed twice.
	 */
	if (evt->nonce == clt->lastnonce &&
	    evt->timestamp == clt->lasttimestamp)
		return;
	clt->lastnonce = evt->nonce;
	clt->lasttimestamp = evt->timestamp;

	/*
	 * If the client is too far behind, drop it.
	 */
	if (io_queued(clt->io) > MQD_CLTBUFMAX) {
		log_warnx("%s: dropping late client", __func__);
		goto close;
	}

	switch(evt->event) {
	case QEV_MSG:
		if (evt->msg->key)
			r = io_printf(clt->io, "%s %llu %zu %s\n",
			    evt->qname, evt->msg->timestamp, evt->msg->datalen,
			    evt->msg->key);
		else
			r = io_printf(clt->io, "%s %llu %zu\n",
			    evt->qname, evt->msg->timestamp, evt->msg->datalen);
		if (r == -1) {
			log_warn("%s: io_printf", __func__);
			goto close;
		}
		r = io_write(clt->io, evt->msg->data, evt->msg->datalen);
		if (r == -1) {
			log_warn("%s: io_write", __func__);
			goto close;
		}
	}
	return;

    close:
	/*
	 * If the client must be closed, do not free it immediatly since
	 * as it might break an iterator that's calling ths function.
	 * Instead mqd_client_flush() will be called at a safe time.
	 */
	io_free(clt->io);
	clt->io = NULL;
	if (clt->state == STATE_MONITOR) {
		TAILQ_REMOVE(&monitors, clt, tqe2);
		if (TAILQ_EMPTY(&monitors))
			queue_del_observer(&monitors);
	}
	TAILQ_INSERT_TAIL(&zombies, clt, tqe2);
}

/*
 * Handle deferred closing of zombie clients.
 */
void
mqd_client_flush(void)
{
	struct mqd_client *clt;

	while ((clt = TAILQ_FIRST(&zombies))) {
		TAILQ_REMOVE(&zombies, clt, tqe2);
		mqd_close(clt);
	}
}

static void
mqd_client_info(struct io *io)
{
	struct mqd_client *clt;
	size_t n;

	n = 0;
	TAILQ_FOREACH(clt, &clients, tqe) {
		io_printf(io, "client.%zu.state=%d\n", n, clt->state);
		io_printf(io, "client.%zu.io.queued=%zu\n", n,
		    io_queued(clt->io));
		io_printf(io, "client.%zu.io.pending=%zu\n", n,
		    io_datalen(clt->io));
		n++;
	}
	io_printf(io, "client.count=%zu\n", n);
}

static void
mqd_dispatch_io(struct io *io, int evt, void *arg)
{
	struct mqd_client *clt = arg;

	switch (evt) {
	case IO_CONNECTED:
	case IO_TLSREADY:
	case IO_TLSERROR:
		return;

	case IO_DATAIN:
		mqd_input(clt);
		return;

	case IO_LOWAT:
		if (clt->state == STATE_QUIT)
			break;
		if (clt->state != STATE_MONITOR &&
		    clt->state != STATE_LISTEN)
			io_set_read(clt->io);
		return;

	case IO_DISCONNECTED:
		log_debug("%08x disconnected", clt->id);
		break;

	case IO_TIMEOUT:
		log_debug("%08x timeout", clt->id);
		break;

	case IO_ERROR:
		log_warnx("%08x io error: %s", clt->id, io_error(io));
		break;

	default:
		fatalx("%s: unexpected event %d", __func__, evt);
	}

	mqd_close(clt);
}

static void
mqd_close(struct mqd_client *clt)
{
	struct mqd_pattern *p;
	uint32_t connid = clt->id;

	if (clt->state == STATE_MONITOR) {
		TAILQ_REMOVE(&monitors, clt, tqe2);
		if (TAILQ_EMPTY(&monitors))
			queue_del_observer(&monitors);
	}

	while ((p = ref_pop(&clt->patterns)))
		pattern_detach_client(p, clt);

	if (clt->msg)
		queue_drop_msg(clt->msg);

	if (clt->io)
		io_free(clt->io);

	TAILQ_REMOVE(&clients, clt, tqe);
	free(clt);
	frontend_conn_closed(connid);
}

static void
mqd_quit(struct mqd_client *clt, const char *err)
{
	if (clt->msg)
		queue_drop_msg(clt->msg);
	clt->msg = NULL;

	clt->state = STATE_QUIT;
	io_print(clt->io, err);
	io_print(clt->io, "\n");
	io_set_write(clt->io);
}

static void
mqd_respond(struct mqd_client *clt, const char *line)
{
	log_debug("%x <<< %s", clt->id, line);
	io_print(clt->io, line);
	io_print(clt->io, "\n");
	io_set_write(clt->io);
}

static void
mqd_input(struct mqd_client *clt)
{
	char *line, *data;
	size_t len;

	for (;;) {
		switch (clt->state) {
		case STATE_READ_COMMAND:
			line = io_getline(clt->io, &len);
			if (line == NULL) {
				if ((io_datalen(clt->io) >= MQD_LINEMAX) ||
				    (line && len >= MQD_LINEMAX)) {
					mqd_quit(clt, "ERR line too long");
					break;
				}
				return;
			}

			mqd_command(clt, line);
			break;

		case STATE_READ_DATA:
			len = io_datalen(clt->io);
			if (len > clt->expect)
				len = clt->expect;
			data = io_data(clt->io);
			memmove(clt->pos, data, len);
			clt->pos = (char *)(clt->pos) + len;
			clt->expect -= len;
			io_drop(clt->io, len);

			if (clt->expect)
				return; /* need more data */

			mqd_cmd_push_done(clt);
			break;

		case STATE_MONITOR:
		case STATE_LISTEN:
		case STATE_QUIT:
			return;
		}

		if (io_queued(clt->io))
			return;
	}
}

static void
mqd_command(struct mqd_client *clt, char *line)
{
#define MAXTOKENS	128
	char *p, *tokens[MAXTOKENS];
	char *last;
	int i = 0;

	log_debug("%x >>> %s", clt->id, line);

	for ((p = strtok_r(line, " ", &last)); p; (p = strtok_r(NULL, " ", &last))) {
		if (i >= MAXTOKENS) {
			mqd_respond(clt, "ERR too many arguments");
			return;
		}
		tokens[i++] = p;
	}
	tokens[i] = NULL;

	if (i == 0)
		mqd_respond(clt, "ERR no command");
	else if (!strcasecmp(tokens[0], "INFO"))
		mqd_cmd_info(clt, i, tokens);
	else if (!strcasecmp(tokens[0], "LISTEN"))
		mqd_cmd_listen(clt, i, tokens);
	else if (!strcasecmp(tokens[0], "MONITOR"))
		mqd_cmd_monitor(clt, i, tokens);
	else if (!strcasecmp(tokens[0], "PSUBSCRIBE"))
		mqd_cmd_subscribe(clt, i, tokens);
	else if (!strcasecmp(tokens[0], "PUSH"))
		mqd_cmd_push(clt, i, tokens);
	else if (!strcasecmp(tokens[0], "QUIT"))
		mqd_cmd_quit(clt, i, tokens);
	else if (!strcasecmp(tokens[0], "SUBSCRIBE"))
		mqd_cmd_subscribe(clt, i, tokens);
	else
		mqd_respond(clt, "ERR unknown command");
}

static void
mqd_cmd_info(struct mqd_client *clt, int argc, char **argv)
{
	if (argc != 1) {
		mqd_quit(clt, "ERR usage: INFO");
		return;
	}

	queue_info(clt->io);
	pattern_info(clt->io);
	mqd_client_info(clt->io);
	io_printf(clt->io, "---\n");
	io_set_write(clt->io);
}

static void
mqd_cmd_listen(struct mqd_client *clt, int argc, char **argv)
{
	if (argc != 1) {
		mqd_quit(clt, "ERR usage: LISTEN");
		return;
	}

	clt->state = STATE_LISTEN;
	io_set_write(clt->io);
}

static void
mqd_cmd_monitor(struct mqd_client *clt, int argc, char **argv)
{
	if (argc != 1) {
		mqd_quit(clt, "ERR usage: MONITOR");
		return;
	}

	if (TAILQ_EMPTY(&monitors))
		queue_add_observer(&monitors, mqd_monitor_observer_cb);
	TAILQ_INSERT_TAIL(&monitors, clt, tqe2);

	clt->state = STATE_MONITOR;
	io_set_write(clt->io);
}

static void
mqd_cmd_push(struct mqd_client *clt, int argc, char **argv)
{
	const char *errstr;
	size_t expect;
	char *key;

	if (argc != 3 && argc != 4) {
		mqd_quit(clt, "ERR usage: PUSH <queue> <length> [key]");
		return;
	}

	if (!queue_validname(argv[1])) {
		mqd_quit(clt, "ERR invalid queue name");
		return;
	}

	expect = strtonum(argv[2], 0, MSG_MAXDATALEN, &errstr);
	if (errstr) {
		mqd_quit(clt, "ERR invalid message size");
		return;
	}

	key = NULL;
	if (argc == 4) {
		key = argv[3];
		if (!queue_validkey(key)) {
			mqd_quit(clt, "ERR invalid key");
			return;
		}
	}

	clt->msg = queue_create_msg(argv[1], expect, key);
	if (clt->msg == NULL) {
		mqd_quit(clt, "ERR internal error");
		return;
	}
	clt->expect = expect;
	clt->pos = clt->msg->data;
	clt->state = STATE_READ_DATA;
}

static void
mqd_cmd_push_done(struct mqd_client *clt)
{
	/* Commit the message */
	queue_push_msg(clt->msg);
	clt->msg = NULL;

	/* Send response */
	mqd_respond(clt, "OK");
	clt->state = STATE_READ_COMMAND;
}

static void
mqd_cmd_quit(struct mqd_client *clt, int argc, char **argv)
{
	if (argc != 1) {
		mqd_respond(clt, "ERR usage: QUIT");
		return;
	}

	mqd_quit(clt, "OK bye");
}

static void
mqd_cmd_subscribe(struct mqd_client *clt, int argc, char **argv)
{
	struct mqd_pattern *p;
	int type, r;

	if (argc != 2) {
		mqd_respond(clt, "ERR usage: %s <channel>");
		return;
	}

	type = PATTERN_EXACT;
	if (!strcasecmp(argv[0], "PSUBSCRIBE"))
		type = PATTERN_FNMATCH;

	p = pattern_get(argv[1], type);
	if (p == NULL) {
		mqd_respond(clt, "ERR internal error");
		return;
	}
	r = pattern_attach_client(p, clt);
	pattern_unref(p);
	if (r == -1) {
		mqd_respond(clt, "ERR internal error");
		return;
	}
	if (r == 1 && ref_add(&clt->patterns, p) == -1) {
		pattern_detach_client(p, clt);
		mqd_respond(clt, "ERR internal error");
		return;
	}

	mqd_respond(clt, "OK");
}

static void
mqd_monitor_observer_cb(void *arg, struct mqd_qevent *evt)
{
	struct mqd_client *clt, *tmp;
	int r;

	TAILQ_FOREACH_SAFE(clt, &monitors, tqe2, tmp) {

		if (io_queued(clt->io) > MQD_CLTBUFMAX) {
			log_warnx("%s: dropping late client", __func__);
			mqd_close(clt);
			continue;
		}

		switch(evt->event) {
		case QEV_NEW:
			r = io_printf(clt->io, "QUEUE NEW %s\n", evt->qname);
			break;

		case QEV_IDLE:
			r = io_printf(clt->io, "QUEUE IDLE %s\n", evt->qname);
			break;

		case QEV_FREE:
			r = io_printf(clt->io, "QUEUE FREE %s\n", evt->qname);
			break;

		case QEV_OPEN:
			r = io_printf(clt->io, "QUEUE OPEN %s\n", evt->qname);
			break;

		case QEV_ROTATE:
			r = io_printf(clt->io, "QUEUE ROTATE %s\n", evt->qname);
			break;

		case QEV_MSG:
			if (evt->msg->key)
				r = io_printf(clt->io, "QUEUE MSG %s %zu %s\n",
				    evt->qname, evt->msg->datalen, evt->msg->key);
			else
				r = io_printf(clt->io, "QUEUE MSG %s %zu\n",
				    evt->qname, evt->msg->datalen);
			break;
		}

		if (r == -1) {
			log_warn("%s: io_printf", __func__);
			mqd_close(clt);
		}
	}
}
