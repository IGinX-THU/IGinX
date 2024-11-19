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
package org.example;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MilvusTest1Test extends TestCase {

    MilvusTest1 test;
    @Before
    public void setUp() {
        test = new MilvusTest1();
        try {
            test.create();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws InterruptedException {
        test.drop();
        test.close();
    }



    @Test
    public void testSearch() throws InterruptedException {
        long r = test.insert();
        assertEquals(r, 10);
        System.out.println("insert test ok");
        long r1= test.search();
        assertEquals(r1, 10);
        System.out.println("search test ok");
    }
}