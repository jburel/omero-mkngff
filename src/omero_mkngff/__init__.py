#!/usr/bin/env python

#
# Copyright (c) 2023 German BioImaging.
# All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from argparse import Namespace

from omero.cli import BaseControl, Parser

HELP = """Plugin to swap OMERO filesets with NGFF

Add your documentation here.

Examples:

    # Do something
    omero mkngff ...

"""


class MkngffControl(BaseControl):
    def _configure(self, parser: Parser) -> None:
        parser.add_login_arguments()
        parser.add_argument(
            "--force",
            "-f",
            default=False,
            action="store_true",
            help="Actually do something. Default: false.",
        )
        parser.add_argument("fileset_id")
        parser.add_argument("zarr_name")
        parser.add_argument("symlink_target")
        parser.set_defaults(func=self.action)

    def action(self, args: Namespace) -> None:
        conn = self.ctx.conn(args)  # noqa
        print(conn)
        self.ctx.out("done.")
