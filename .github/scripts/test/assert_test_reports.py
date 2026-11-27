#!/usr/bin/env python3
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

"""Fail on missing, skipped-only or failed JUnit reports; accepts path:minimum pairs."""
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

for spec in sys.argv[1:]:
    name, minimum = spec.rsplit(":", 1)
    report = Path(name)
    if not report.is_file():
        raise SystemExit("Missing test report: " + name)
    suite = ET.parse(report).getroot()
    executed = int(suite.get("tests", 0)) - int(suite.get("skipped", 0))
    if executed < int(minimum) or int(suite.get("failures", 0)) or int(suite.get("errors", 0)):
        raise SystemExit("Unsuccessful test report: " + name)
    print(f"{suite.get('name')}: {executed} executed")
