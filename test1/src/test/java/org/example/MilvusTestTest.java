package org.example;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MilvusTestTest extends TestCase {

    MilvusTest test;
    @Before
    public void setUp() {
        test = new MilvusTest();
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