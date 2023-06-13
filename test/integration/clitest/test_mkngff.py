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

from omero.plugins.mkngff import MkngffControl
from omero.testlib.cli import CLITest


class TestMkngff(CLITest):
    def setup_method(self, method):  # type: ignore
        super().setup_method(method)
        self.cli.register("mkngff", MkngffControl, "TEST")
        self.args += ["mkngff"]

    def mkngff(self, capfd):  # type: ignore
        self.cli.invoke(self.args, strict=True)
        return capfd.readouterr()[0]

    def test_mkngff(self, capfd):  # type: ignore
        name = self.uuid()
        object_type = "Project"
        oid = self.create_object(object_type, name=f"{name}")
        obj_arg = f"{object_type}:{oid}"
        self.args += [obj_arg]
        out = self.rdf(capfd)
        assert out
