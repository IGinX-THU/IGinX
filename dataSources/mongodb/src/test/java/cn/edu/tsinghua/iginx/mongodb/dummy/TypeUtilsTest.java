/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.mongodb.dummy;

import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class TypeUtilsTest {

  @Test
  public void jsonCodec() {
    // test BsonBoolean
    BsonBoolean bsonBoolean = new BsonBoolean(true);
    String boolJson = TypeUtils.toJson(bsonBoolean);
    Assert.assertEquals("true", boolJson);
    BsonValue bsonBooleanParsed = TypeUtils.parseJson(boolJson);
    Assert.assertEquals(bsonBoolean, bsonBooleanParsed);

    // test BsonInt32
    BsonInt32 bsonInt32 = new BsonInt32(1);
    String intJson = TypeUtils.toJson(bsonInt32);
    Assert.assertEquals("1", intJson);
    BsonValue bsonInt32Parsed = TypeUtils.parseJson(intJson);
    Assert.assertEquals(new BsonInt32(1), bsonInt32Parsed);

    // test BsonInt64
    BsonInt64 bsonInt64 = new BsonInt64(Long.MAX_VALUE);
    String longJson = TypeUtils.toJson(bsonInt64);
    Assert.assertEquals(String.valueOf(Long.MAX_VALUE), longJson);
    BsonValue bsonInt64Parsed = TypeUtils.parseJson(longJson);
    Assert.assertEquals(bsonInt64, bsonInt64Parsed);

    // test BsonDouble
    BsonDouble bsonDouble = new BsonDouble(1.0);
    String doubleJson = TypeUtils.toJson(bsonDouble);
    Assert.assertEquals("1.0", doubleJson);
    BsonValue bsonDoubleParsed = TypeUtils.parseJson(doubleJson);
    Assert.assertEquals(bsonDouble, bsonDoubleParsed);

    // test BsonString
    BsonString bsonString = new BsonString("test");
    String strJson = TypeUtils.toJson(bsonString);
    Assert.assertEquals("test", strJson);
    BsonValue bsonStringParsed = TypeUtils.parseJson(strJson);
    Assert.assertEquals(bsonString, bsonStringParsed);

    // test Binary
    BsonBinary bsonBinary = new BsonBinary(new byte[] {1, 2, 3});
    String binaryJson = TypeUtils.toJson(bsonBinary);
    Assert.assertEquals(
        "new BinData(0, \"AQID\")", binaryJson); // "AQID" is base64 encoded "1, 2, 3"
    BsonValue bsonBinaryParsed = TypeUtils.parseJson(binaryJson);
    Assert.assertEquals(bsonBinary, bsonBinaryParsed);

    // test BsonObjectId
    BsonObjectId bsonObjectId = new BsonObjectId(new ObjectId());
    String oidJson = TypeUtils.toJson(bsonObjectId);
    Assert.assertEquals("ObjectId(\"" + bsonObjectId.getValue().toHexString() + "\")", oidJson);
    BsonValue bsonObjectIdParsed = TypeUtils.parseJson(oidJson);
    Assert.assertEquals(bsonObjectId, bsonObjectIdParsed);

    // test BsonPattern
    BsonRegularExpression bsonPattern = new BsonRegularExpression(".*");
    String patternJson = TypeUtils.toJson(bsonPattern);
    Assert.assertEquals("/.*/", patternJson);
    BsonValue bsonPatternParsed = TypeUtils.parseJson(patternJson);
    Assert.assertEquals(bsonPattern, bsonPatternParsed);
  }
}
