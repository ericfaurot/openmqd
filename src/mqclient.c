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
#include <sys/socket.h>
#include <sys/time.h>

#include <err.h>
#include <errno.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <vis.h>

#define MQD_PORT        6694

int main_mqclient(int, char **);

static int mq_connect(const char *, const char *);
static char *mq_dialog(FILE *, const char *, ...);
static char *mq_getline(FILE *);

static size_t push_count;
static size_t push_size;
static struct timespec t0;
static struct timespec t1;
static struct timespec timer;

static void
usage(void)
{
	extern char *__progname;

	fprintf(stderr,
	    "usage: %s [-h host] [-k key] [-p port] [-q name] <cmd> ...\n",
	    __progname);
	exit(1);
}

int
main_mqclient(int argc, char **argv)
{
	const char *host = "localhost";
	const char *port = "6694";
	const char *qname = NULL;
	const char *key = NULL;
	FILE *fp;
	char *line = NULL, *resp;
	unsigned char *buf = NULL;
	size_t buflen = 0, bufsz = 0, linesize = 0;
	ssize_t linelen;
	int ch, sock;

	while ((ch = getopt(argc, argv, "h:k:p:q:")) != -1) {
		switch (ch) {
		case 'k':
			key = optarg;
			break;
		case 'h':
			host = optarg;
			break;
		case 'p':
			port = optarg;
			break;
		case 'q':
			qname = optarg;
			break;
		default:
			usage();
			/* NOTREACHED */
		}
	}
	argc -= optind;
	argv += optind;

	if (argc == 0)
		usage();

	sock = mq_connect(host, port);
	if ((fp = fdopen(sock, "rw+")) == NULL)
		err(1, "fopen");

	if (!strcmp(argv[0], "info")) {

		if (fprintf(fp, "INFO\n") == -1)
			err(1, "fprintf");

		for (;;) {
			resp = mq_getline(fp);
			if (!strcmp(resp, "---"))
				break;
			printf("%s\n", resp);
		}

	}

	else if (!strcmp(argv[0], "monitor")) {

		if (fprintf(fp, "MONITOR\n") == -1)
			err(1, "fprintf");
		for (;;) {
			resp = mq_getline(fp);
			printf("%s\n", resp);
		}

		return 0;
	}

	else if (!strcmp(argv[0], "push")) {

		if (qname == NULL)
			errx(1, "no queue specified");

		while ((ch = fgetc(stdin)) != EOF) {
			if (bufsz == buflen) {
				bufsz += 1024;
				buf = reallocarray(buf, bufsz, 1);
				if (buf == NULL)
					err(1, "reallocarray");
			}
			buf[buflen++] = ch;
		}
		if (ferror(stdin))
			err(1, "fgetc");

		if (key) {
			if (fprintf(fp, "PUSH %s %zu %s\n", qname, buflen, key) == -1)
				err(1, "fprintf");
		}
		else
			if (fprintf(fp, "PUSH %s %zu\n", qname, buflen) == -1)
				err(1, "fprintf");
		if (buflen) {
			if (fwrite(buf, 1, buflen, fp) != buflen)
				err(1, "fwrite");
		}
		if (fflush(fp) == -1)
			err(1, "fflush");
		resp = mq_getline(fp);

		if (strcmp(resp, "OK"))
			printf("PUSH %s %zu: %s\n", qname, buflen, resp);
	}

	else if (!strcmp(argv[0], "pushlines")) {

		if (qname == NULL)
			errx(1, "no queue specified");

		while ((linelen = getline(&line, &linesize, stdin)) != -1) {

			if (line[linelen - 1] == '\n')
				line[--linelen ] = '\0';
			buflen = linelen;

			clock_gettime(CLOCK_MONOTONIC, &t0);

			if (key) {
				if (fprintf(fp, "PUSH %s %zu %s\n", qname, buflen, key) == -1)
					err(1, "fprintf");
			}
			else
				if (fprintf(fp, "PUSH %s %zu\n", qname, buflen) == -1)
					err(1, "fprintf");
			if (buflen) {
				if (fwrite(line, 1, buflen, fp) != buflen)
					err(1, "fwrite");
			}
			if (fflush(fp) == -1)
				err(1, "fflush");
			resp = mq_getline(fp);

			clock_gettime(CLOCK_MONOTONIC, &t1);

			timespecsub(&t1, &t0, &t0);
			timespecadd(&timer, &t0, &timer);

			if (strcmp(resp, "OK"))
				printf("PUSH %s %zu: %s\n", qname, buflen, resp);

			push_count += 1;
			push_size += buflen;
		}

		if (ferror(stdin)) {
			err(1, "stdin");
		}

		double df;
		df = timer.tv_sec;
		df += (timer.tv_nsec / 1000000000.0);

		printf("pushed %zu bytes for %zu msg in %.03lfs\n", push_size, push_count, df);

	}

	return 0;
}

static int
mq_connect(const char *host, const char *port)
{
	struct addrinfo hints, *res, *res0;
	int error;
	int save_errno;
	int s;
	const char *cause = NULL;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = PF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	error = getaddrinfo(host, port, &hints, &res0);
	if (error)
		errx(1, "%s:%s: %s", host, port, gai_strerror(error));
	s = -1;
	for (res = res0; res; res = res->ai_next) {
		s = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		if (s == -1) {
			cause = "socket";
			continue;
		}

		if (connect(s, res->ai_addr, res->ai_addrlen) == -1) {
			cause = "connect";
			save_errno = errno;
			close(s);
			errno = save_errno;
			s = -1;
			continue;
		}

		break;  /* okay we got one */
	}
	if (s == -1)
		err(1, "%s: %s:%s", cause, host, port);

	freeaddrinfo(res0);

	return s;
}

static char *
mq_dialog(FILE *f, const char *fmt, ...)
{
	va_list ap;
	int n;

	va_start(ap, fmt);
	n = vfprintf(f, fmt, ap);
	va_end(ap);

	if (n == -1)
		err(1, "vfprintf");
	if (fputc('\n', f) != '\n')
		err(1, "fputc");
	if (fflush(f) == -1)
		err(1, "fflush");

	return mq_getline(f);
}

static char *
mq_getline(FILE *f)
{
	static char *line = NULL;
	static size_t linesz = 0;
	ssize_t len;

	len = getline(&line, &linesz, f);
	if (len == -1)
		err(1, "getline");

	if (line[len - 1] == '\n')
		line[len - 1] = '\0';

	return line;
}
