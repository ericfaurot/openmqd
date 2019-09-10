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
#include <sys/queue.h>
#include <sys/tree.h>
#include <sys/uio.h>
#include <sys/socket.h>

#include <event.h>
#include <imsg.h>
#include <limits.h>
#include <netdb.h>

#define	PORT_MQD	6694

#define	MQD_CONFIG	"/etc/mqd.conf"
#define	MQD_SOCKET	"/var/run/mqd.sock"
#define	MQD_QUEUE	"/var/mqd"
#define	MQD_USER	"daemon"
#define	MQD_LINEMAX	1024
#define	MQD_MSGSIZEMAX	32768

#define MQD_CLTBUFMAX	4194304

#define	LOGLEVEL_CONN	2
#define	LOGLEVEL_IMSG	3
#define	LOGLEVEL_IO	4

enum {
	IMSG_NONE,

	IMSG_SOCK_ENGINE,
	IMSG_SOCK_FRONTEND,

	IMSG_CONF_START,
	IMSG_CONF_LISTENER,
	IMSG_CONF_END,

	IMSG_RES_GETADDRINFO,
	IMSG_RES_GETADDRINFO_END,
	IMSG_RES_GETNAMEINFO,

	IMSG_MQ_SCAN,
	IMSG_MQ_OPEN,
	IMSG_MQ_UNLINK,
};

enum {
	PROC_CLIENT,
	PROC_CONTROL,
	PROC_ENGINE,
	PROC_FRONTEND,
	PROC_PRIV
};

enum {
	PROTO_NONE = 0,
	PROTO_MQD
};

struct listener {
	TAILQ_ENTRY(listener)	 entry;
	int			 sock;
	int			 proto;
	struct sockaddr_storage	 ss;
	struct timeval		 timeout;
	struct event		 ev;
	int			 pause;
};

struct mqd_conf {
	TAILQ_HEAD(, listener)	 listeners;
};

struct io;
struct imsgproc;

#define PATTERN_ALL	0
#define PATTERN_EXACT	1
#define PATTERN_FNMATCH	2

struct mqd_queue;
struct mqd_pattern;
struct mqd_client;

struct msg {
	SIMPLEQ_ENTRY(msg)	 entry;
	struct mqd_queue	*queue;
	uint64_t		 timestamp;
	char			*key;
	char			*data;
	size_t			 datalen;
};

enum {
	QEV_NEW = 0,		/* new queue created */
	QEV_IDLE,		/* queue becomes idle */
	QEV_FREE,		/* queue is discarded */
	QEV_OPEN,		/* persistent queue gets opened */
	QEV_ROTATE,		/* persistent queue gets rotated */
	QEV_MSG,		/* a message is pushed to the queue */
};

struct mqd_qevent {
	int			 event;
	int			 nonce;
	uint64_t		 timestamp;
	const char		*qname;
	struct mqd_queue	*queue;
	struct msg		*msg;
};

extern struct mqd_conf *env;
extern struct imsgproc *p_control;
extern struct imsgproc *p_engine;
extern struct imsgproc *p_frontend;
extern struct imsgproc *p_priv;

/* control.c */
void control(int, int);

/* engine.c */
void engine(int, int);

/* frontend.c */
void frontend(int, int);
void frontend_conn_closed(uint32_t);

/* frontend_mqd.c */
void frontend_mqd_init(void);
void frontend_mqd_shutdown(void);
void frontend_mqd_conn(uint32_t, struct listener *, int, const struct sockaddr *);
void frontend_mqd_dispatch(struct imsgproc *, struct imsg *);
void mqd_client_qevent(struct mqd_client *, struct mqd_qevent *);
void mqd_client_flush(void);

/* logmsg.c */
const char *log_fmt_proto(int);
const char *log_fmt_imsgtype(int);
const char *log_fmt_proctype(int);
const char *log_fmt_sockaddr(const struct sockaddr *);
void log_imsg(struct imsgproc *, struct imsg *);
void log_io(const char *, struct io *, int);

/* parse.y */
struct mqd_conf *parse_config(const char *, int);
int cmdline_symset(char *);

/* pattern.c */
void pattern_init(void);
void pattern_shutdown(void);
void pattern_info(struct io *);
struct mqd_pattern *pattern_get(const char *, int);
void pattern_ref(struct mqd_pattern *);
void pattern_unref(struct mqd_pattern *);
int pattern_attach_client(struct mqd_pattern *, struct mqd_client *);
int pattern_detach_client(struct mqd_pattern *, struct mqd_client *);
void pattern_qevent(struct mqd_pattern *, struct mqd_qevent *);

/* queue.c */
void queue_init(void);
void queue_shutdown(void);
void queue_dispatch(struct imsgproc *, struct imsg *);
int queue_validname(const char *);
int queue_validkey(const char *);
void queue_info(struct io *);
const char *queue_name(struct mqd_queue *);
int queue_iter(void **, struct mqd_queue **);
struct msg *queue_create_msg(const char *, size_t, const char *);
void queue_push_msg(struct msg *);
void queue_drop_msg(struct msg *);
int queue_add_observer(void *, void (*)(void *, struct mqd_qevent *));
void queue_del_observer(void *);
int queue_attach_pattern(struct mqd_queue *, struct mqd_pattern *);
int queue_detach_pattern(struct mqd_queue *, struct mqd_pattern *);

/* ref.c */
struct refentry;
SPLAY_HEAD(refs, refentry);
#define ref_init(s) SPLAY_INIT(s)
int ref_add(struct refs *, const void *);
int ref_del(struct refs *, const void *);
void *ref_pop(struct refs *);
void *ref_iter(struct refs *, void **);
size_t ref_count(struct refs *);

/* resolver.c */
void resolver_getaddrinfo(const char *, const char *, const struct addrinfo *,
    void(*)(void *, int, struct addrinfo*), void *);
void resolver_getnameinfo(const struct sockaddr *, int,
    void(*)(void *, int, const char *, const char *), void *);
void resolver_dispatch_request(struct imsgproc *, struct imsg *);
void resolver_dispatch_result(struct imsgproc *, struct imsg *);

/* util.c */
int validqname(const char *);
