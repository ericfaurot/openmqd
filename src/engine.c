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

#include <sys/stat.h>

#include <errno.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "mqd.h"
#include "mq.h"

#include "log.h"
#include "proc.h"

static void engine_shutdown(void);
static void engine_dispatch_priv(struct imsgproc *, struct imsg *, void *);
static void engine_dispatch_frontend(struct imsgproc *, struct imsg *, void *);

void
engine(int debug, int verbose)
{
	struct passwd *pw;
	struct stat st;

	/* Early initialisation. */
	log_init(debug, LOG_DAEMON);
	log_setverbose(verbose);
	log_procinit("engine");
	setproctitle("engine");

	/* Drop priviledges. */
	if ((pw = getpwnam(MQD_USER)) == NULL)
		fatal("%s: getpwnam: %s", __func__, MQD_USER);

	if (mkdir(MQD_QUEUE, 0700) == -1) {
		if (errno != EEXIST)
			fatal("%s: mkdir: %s", __func__, MQD_QUEUE);
		if (stat(MQD_QUEUE, &st) == -1)
			fatal("%s: stat: %s", __func__, MQD_QUEUE);
		if (!S_ISDIR(st.st_mode))
			fatalx("%s is not a directory", MQD_QUEUE);
		if (st.st_uid != pw->pw_uid)
			fatalx("%s must be owned by %s", MQD_QUEUE, MQD_USER);
		if ((st.st_mode & 0027) != 0)
			fatalx("%s must be at most 0750", MQD_QUEUE);
	}
	else
		if (chown(MQD_QUEUE, pw->pw_uid, pw->pw_gid) == -1)
			fatal("%s: chown", __func__);

	if (setgroups(1, &pw->pw_gid) ||
	    setresgid(pw->pw_gid, pw->pw_gid, pw->pw_gid) ||
	    setresuid(pw->pw_uid, pw->pw_uid, pw->pw_uid))
		fatal("%s: cannot drop privileges", __func__);

	if (pledge("stdio rpath wpath cpath dns sendfd recvfd", NULL) == -1)
		fatal("%s: pledge", __func__);

	event_init();

	signal(SIGPIPE, SIG_IGN);
	signal(SIGINT, SIG_IGN);

	/* Setup imsg socket with parent. */
	p_priv = proc_attach(PROC_PRIV, 3);
	if (p_priv == NULL)
		fatal("%s: proc_attach", __func__);
	proc_setcallback(p_priv, engine_dispatch_priv, NULL);
	proc_enable(p_priv);

	event_dispatch();

	engine_shutdown();
}

static void
engine_shutdown()
{
	log_debug("exiting");

	exit(0);
}

static void
engine_dispatch_priv(struct imsgproc *proc, struct imsg *imsg, void *arg)
{
	if (imsg == NULL) {
		log_debug("%s: imsg connection lost", __func__);
		event_loopexit(NULL);
		return;
	}

	if (log_getverbose() > LOGLEVEL_IMSG)
		log_imsg(proc, imsg);

	switch (imsg->hdr.type) {
	case IMSG_SOCK_FRONTEND:
		m_end(proc);

		if (imsg->fd == -1)
			fatalx("failed to receive frontend socket");
		p_frontend = proc_attach(PROC_FRONTEND, imsg->fd);
		proc_setcallback(p_frontend, engine_dispatch_frontend, NULL);
		proc_enable(p_frontend);
		break;

	case IMSG_CONF_START:
		m_end(proc);
		break;

	case IMSG_CONF_END:
		m_end(proc);
		break;

	default:
		fatalx("%s: unexpected imsg %s", __func__,
		    log_fmt_imsgtype(imsg->hdr.type));
	}
}

static void
engine_dispatch_frontend(struct imsgproc *proc, struct imsg *imsg, void *arg)
{
	const char *qname;
	uint64_t offset;
	char path[PATH_MAX];
	int fd, mode, r;

	if (imsg == NULL) {
		log_debug("%s: imsg connection lost", __func__);
		event_loopexit(NULL);
		return;
	}

	if (log_getverbose() > LOGLEVEL_IMSG)
		log_imsg(proc, imsg);

	switch (imsg->hdr.type) {
	case IMSG_RES_GETADDRINFO:
	case IMSG_RES_GETNAMEINFO:
		resolver_dispatch_request(proc, imsg);
		break;

	case IMSG_MQ_SCAN:
		m_get_string(proc, &qname);
		m_end(proc);

		fd = -1;
		snprintf(path, sizeof(path), "%s/%s", MQD_QUEUE, qname);
		r = mq_scan(path, &offset);
		if (r == -1 && errno == ENOENT) {
			if (mkdir(path, 0700) == -1)
				log_warn("%s: mkdir: %s", __func__, path);
			else
				r = mq_scan(path, &offset);
		}

		if (r == -1)
			log_warn("mq_scan: %s", qname);
		else if ((fd = mq_open(path, offset, MQ_WRITE)) == -1)
			log_warn("mq_open: %s", qname);

		m_create(proc, IMSG_MQ_SCAN, 0, 0, fd);
		m_add_string(proc, qname);
		m_add_u64(proc, offset);
		m_add_int(proc, MQ_WRITE);
		m_close(proc);
		break;

	case IMSG_MQ_OPEN:
		m_get_string(proc, &qname);
		m_get_u64(proc, &offset);
		m_get_int(proc, &mode);
		m_end(proc);

		snprintf(path, sizeof(path), "%s/%s", MQD_QUEUE, qname);
		fd = mq_open(qname, offset, mode);

		m_create(proc, IMSG_MQ_OPEN, 0, 0, fd);
		m_add_string(proc, qname);
		m_add_u64(proc, offset);
		m_add_int(proc, mode);
		m_close(proc);
		break;

	case IMSG_MQ_UNLINK:
		m_get_string(proc, &qname);
		m_get_u64(proc, &offset);
		m_end(proc);

		snprintf(path, sizeof(path), "%s/%s", MQD_QUEUE, qname);
		mq_unlink(path, offset);
		break;

	default:
		fatalx("%s: unexpected imsg %s", __func__,
		    log_fmt_imsgtype(imsg->hdr.type));
	}
}
