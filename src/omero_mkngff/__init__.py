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
from pathlib import Path
from typing import Generator, Tuple

import omero.all  # noqa
from omero.cli import BaseControl, Parser
from omero.sys import ParametersI

HELP = """Plugin to swap OMERO filesets with NGFF

CLI plugin used to swap an existing OMERO fileset with

Examples:

    # Generate SQL needed for initial setup
    omero mkngff setup

    # Generate SQL for converting the given fileset
    omero mkngff sql ${fileset} ${zarrdir}

    # ... while overriding the name of the directory under the ManagedRepository
    omero mkngff sql ${fileset} ${zarrdir} --zarr_name "nice.ome.zarr"

"""


SETUP = """

CREATE OR REPLACE FUNCTION mkngff_fileset(
    old_fileset bigint,
    uuid character varying,
    repo character varying,
    prefix character varying,
    info text[][])
  RETURNS integer AS
$BODY$
DECLARE
   fs_id integer;
   file_id integer;
   ann_id integer;
BEGIN

    insert into fileset
        (id, permissions, templateprefix, creation_id, group_id, owner_id, update_id)
        values
        (nextval('seq_fileset'), -120, prefix, 497, 0, 0, 497)
        returning id into fs_id;

    insert into annotation
        (id, permissions, creation_id, group_id, owner_id, update_id,
          ns, longvalue, discriminator)
        values
        (nextval('seq_annotation'), -120, 497, 0, 0, 497,
          'mkngff', old_fileset, '/basic/num/long/')
        returning id into ann_id;

    insert into filesetannotationlink
        (id, permissions, creation_id, group_id, owner_id, update_id,
          parent, child)
        values
        (nextval('seq_filesetannotationlink'), -120, 497, 0, 0, 497,
          fs_id, ann_id);

    for i in 1 .. array_upper(info, 1)
    loop

      insert into originalfile
          (id, permissions, creation_id, owner_id, group_id, update_id
            mimetype, repo, path, name)
          values (nextval('seq_originalfile'), -120, 497, 0, 0, 500,
            info[i][3], repo, info[i][1], uuid || info[i][2])
          returning id into file_id;

      insert into filesetentry
          (id, permissions, creation_id, update_id, owner_id, group_id,
            fileset, originalfile, fileset_index, clientpath)
          values (nextval('seq_filesetentry'), -120, 497, 497, 0, 0,
            fs_id, file_id, i-1, 'unknown');

    end loop;

    update image set fileset = fs_id where fileset = old_fileset;

    RETURN fs_id;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;

"""

TEMPLATE = """
begin;
    select mkngff_fileset(
      {OLD_FILESET},
      '{UUID}',
      '{REPO}',
      '{PREFIX}',
      array[
{ROWS}
      ]::text[][]
    );
commit;
"""

ROW = """          ['{PATH}', '{NAME}', '{MIME}']"""


class MkngffControl(BaseControl):
    def _configure(self, parser: Parser) -> None:
        parser.add_login_arguments()
        sub = parser.add_subparsers()

        setup = sub.add_parser("setup", help="print SQL setup statement")
        setup.set_defaults(func=self.setup)

        sql = sub.add_parser("sql", help="generate SQL statement")
        sql.add_argument(
            "--secret", help="DB UUID for protecting SQL statements", default="TBD"
        )
        sql.add_argument("--zarr_name", help="Nicer name for zarr directory if desired")
        sql.add_argument("fileset_id", type=int)
        sql.add_argument("symlink_target")
        sql.set_defaults(func=self.sql)

    def setup(self, args: Namespace) -> None:
        self.ctx.out(SETUP)

    def sql(self, args: Namespace) -> None:
        conn = self.ctx.conn(args)  # noqa
        q = conn.sf.getQueryService()
        rv = q.findAllByQuery(
            (
                "select f from Fileset f join fetch f.usedFiles fe "
                "join fetch fe.originalFile ofile where f.id = :id"
            ),
            ParametersI().addId(args.fileset_id),
        )
        if len(rv) != 1:
            self.ctx.die(400, f"Found wrong number of filesets: {len(rv)}")
            return

        prefix = rv[0].templatePrefix.val

        if prefix.endswith("/"):
            prefix = prefix[:-1]  # Drop ending "/"

        prefix_path, prefix_name = prefix.rsplit("/", 1)
        self.ctx.err(
            f"Found prefix {prefix_path} // {prefix_name} for fileset {args.fileset_id}"
        )

        symlink_path = Path(args.symlink_target)

        if not symlink_path.exists():
            self.ctx.die(401, f"Symlink target does not exist: {args.symlink_target}")
            return

        zarr_name = args.zarr_name
        if not zarr_name:
            zarr_name = symlink_path.name

        rows = []
        rows.append(
            ROW.format(
                PATH=f"{prefix_path}/",
                NAME=f"{prefix_name}_converted",
                MIME="Directory",
            )
        )
        rows.append(
            ROW.format(
                PATH=f"{prefix_path}/{prefix_name}_converted/",
                NAME=zarr_name,
                MIME="Directory",
            )
        )
        for row_path, row_name, row_mime in self.walk(symlink_path):
            rows.append(
                ROW.format(
                    PATH=f"{prefix_path}/{prefix_name}_converted/{row_path}/",
                    NAME=row_name,
                    MIME=row_mime,
                )
            )

        self.ctx.out(
            TEMPLATE.format(
                OLD_FILESET=args.fileset_id,
                PREFIX=f"{prefix_path}/{prefix_name}_converted/{zarr_name}/",
                ROWS=",\n".join(rows),
                REPO=self.get_uuid(args),
                UUID=args.secret,
            )
        )

    def walk(self, path: Path) -> Generator[Tuple[Path, str, str], None, None]:
        for p in path.iterdir():
            if not p.is_dir():
                yield (p.parent, p.name, "application/octet-stream")
            else:
                if (p / ".zarray").exists() or (p / ".zgroup").exists():
                    yield (p.parent, p.name, "Directory")
                    yield from self.walk(p)
                else:
                    # Chunk directory
                    continue

    def get_uuid(self, args: Namespace) -> str:
        from omero.grid import ManagedRepositoryPrx as MRepo

        client = self.ctx.conn(args)
        shared = client.sf.sharedResources()
        repos = shared.repositories()
        repos = list(zip(repos.descriptions, repos.proxies))

        for idx, pair in enumerate(repos):
            desc, prx = pair
            is_mrepo = MRepo.checkedCast(prx)
            if is_mrepo:
                return desc.hash.val

        raise self.ctx.die(
            402, f"Failed to find managed repository (count={len(repos)})"
        )
