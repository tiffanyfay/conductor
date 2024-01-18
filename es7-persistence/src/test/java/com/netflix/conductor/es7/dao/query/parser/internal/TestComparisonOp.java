/*
 * Copyright 2023 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.es7.dao.query.parser.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Viren
 */
class TestComparisonOp extends AbstractParserTest {

    @Test
    void test() throws Exception {
        String[] tests = new String[] {"<", ">", "=", "!=", "IN", "BETWEEN", "STARTS_WITH"};
        for (String test : tests) {
            ComparisonOp name = new ComparisonOp(getInputStream(test));
            String nameVal = name.getOperator();
            assertNotNull(nameVal);
            assertEquals(test, nameVal);
        }
    }

    @Test
    void invalidOp() throws Exception {
        assertThrows(ParserException.class, () -> {
            String test = "AND";
            ComparisonOp name = new ComparisonOp(getInputStream(test));
            String nameVal = name.getOperator();
            assertNotNull(nameVal);
            assertEquals(test, nameVal);
        });
    }
}
