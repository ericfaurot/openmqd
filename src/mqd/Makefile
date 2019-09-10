#	$OpenBSD: Makefile,v 1.74 2014/04/15 20:55:42 miod Exp $

.PATH:		${.CURDIR}/..

PROG=	mqd

SRCS+=	control.c
SRCS+=	engine.c
SRCS+=	frontend.c
SRCS+=	frontend_mqd.c
SRCS+=	io.c
SRCS+=	iobuf.c
SRCS+=	log.c
SRCS+=	logmsg.c
SRCS+=	mq.c
SRCS+=	mqd.c
SRCS+=	parse.y
SRCS+=	pattern.c
SRCS+=	proc.c
SRCS+=	queue.c
SRCS+=	ref.c
SRCS+=	resolver.c
SRCS+=	shbuf.c
SRCS+=	util.c

NOMAN=	noman

LDADD+=	-levent -lutil -ltls
DPADD+=	${LIBEVENT} ${LIBUTIL}

.include <bsd.prog.mk>
