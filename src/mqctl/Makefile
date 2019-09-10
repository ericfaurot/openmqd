#	$OpenBSD: Makefile,v 1.74 2014/04/15 20:55:42 miod Exp $

.PATH:		${.CURDIR}/..

PROG=	mqctl

SRCS+=	mq.c
SRCS+=	mqctl.c
SRCS+=	mqclient.c
SRCS+=	mqdump.c
SRCS+=	mqtest.c

NOMAN=	noman

LDADD+=	-lutil
DPADD+=	${LIBUTIL}

LINKS=	${DESTDIR}/usr/sbin/mqctl ${DESTDIR}/usr/sbin/mqclient
LINKS+=	${DESTDIR}/usr/sbin/mqctl ${DESTDIR}/usr/sbin/mqdump
LINKS+=	${DESTDIR}/usr/sbin/mqctl ${DESTDIR}/usr/sbin/mqtest

.include <bsd.prog.mk>
