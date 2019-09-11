/*	$OpenBSD$	*/
/*
 * Copyright (c) 2015 Eric Faurot <eric@openbsd.org>
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

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main_mqclient(int, char **);
int main_mqdump(int, char **);
int main_mqtest(int, char **);

extern char *__progname;

static void
usage(void)
{
	fprintf(stderr, "usage: %s [-f conf] <cmd> ...\n", __progname);
	exit(1);
}

int
main(int argc, char **argv)
{
	char *conffile = "/etc/mqd.conf";
	int ch;

	if (!strcmp(__progname, "mqclient"))
		return main_mqclient(argc, argv);
	if (!strcmp(__progname, "mqdump"))
		return main_mqdump(argc, argv);
	if (!strcmp(__progname, "mqtest"))
		return main_mqtest(argc, argv);

	while ((ch = getopt(argc, argv, "f:")) != -1) {
		switch (ch) {
		case 'f':
			conffile = optarg;
			break;
		default:
			usage();
			/* NOTREACHED */
		}
	}
	argc -= optind;
	argv += optind;

	return 0;
}
