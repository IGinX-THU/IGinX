/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.client.constant;

import java.util.regex.Pattern;

public class Constants {

  public static final String IGINX_CLI_PREFIX = "IGinX> ";
  public static final String IGINX_CLI_PREFIX_WAITING_INPUT = "     > ";
  public static final String IGINX_CLI_PREFIX_SINGLE_QUOTE = "    '> ";
  public static final String IGINX_CLI_PREFIX_DOUBLE_QUOTE = "    \"> ";
  public static final String IGINX_CLI_PREFIX_BACKTICK_QUOTE = "    `> ";
  public static final String IGINX_CLI_PREFIX_BLOCK_COMMENT = "   /*> ";

  public static final String HOST_ARGS = "h";
  public static final String HOST_NAME = "host";

  public static final String PORT_ARGS = "p";
  public static final String PORT_NAME = "port";

  public static final String USERNAME_ARGS = "u";
  public static final String USERNAME_NAME = "username";

  public static final String PASSWORD_ARGS = "pw";
  public static final String PASSWORD_NAME = "password";

  public static final String EXECUTE_ARGS = "e";
  public static final String EXECUTE_NAME = "execute";

  public static final String FETCH_SIZE_ARGS = "fs";
  public static final String FETCH_SIZE_NAME = "fetch_size";

  public static final String HELP_ARGS = "help";

  public static final int MAX_HELP_CONSOLE_WIDTH = 88;

  public static final String SCRIPT_HINT = "./start-cli.sh(start-cli.bat if Windows)";

  public static final Pattern EXIT_OR_QUIT_PATTERN =
      Pattern.compile("^\\s*(exit|quit)\\s*;\\s*$", Pattern.CASE_INSENSITIVE);
}
