/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.avro.io.parsing;

import com.avro.Schema;
import com.avro.Schema.Field;

import java.util.HashMap;
import java.util.Map;

/**
 * The class that generates a grammar suitable to parse Avro data
 * in JSON format.
 */

public class JsonGrammarGenerator extends ValidatingGrammarGenerator {
  /**
   * Returns the non-terminal that is the start symbol
   * for the grammar for the grammar for the given schema <tt>sc</tt>.
   */
  public com.avro.io.parsing.Symbol generate(Schema schema) {
    return com.avro.io.parsing.Symbol.root(generate(schema, new HashMap<ValidatingGrammarGenerator.LitS, com.avro.io.parsing.Symbol>()));
  }

  /**
   * Returns the non-terminal that is the start symbol
   * for grammar of the given schema <tt>sc</tt>. If there is already an entry
   * for the given schema in the given map <tt>seen</tt> then
   * that entry is returned. Otherwise a new symbol is generated and
   * an entry is inserted into the map.
   * @param sc    The schema for which the start symbol is required
   * @param seen  A map of schema to symbol mapping done so far.
   * @return      The start symbol for the schema
   */
  public com.avro.io.parsing.Symbol generate(Schema sc, Map<ValidatingGrammarGenerator.LitS, com.avro.io.parsing.Symbol> seen) {
    switch (sc.getType()) {
    case NULL:
    case BOOLEAN:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case STRING:
    case BYTES:
    case FIXED:
    case UNION:
      return super.generate(sc, seen);
    case ENUM:
      return com.avro.io.parsing.Symbol.seq(com.avro.io.parsing.Symbol.enumLabelsAction(sc.getEnumSymbols()),
          com.avro.io.parsing.Symbol.ENUM);
    case ARRAY:
      return com.avro.io.parsing.Symbol.seq(com.avro.io.parsing.Symbol.repeat(com.avro.io.parsing.Symbol.ARRAY_END,
              com.avro.io.parsing.Symbol.ITEM_END, generate(sc.getElementType(), seen)),
          com.avro.io.parsing.Symbol.ARRAY_START);
    case MAP:
      return com.avro.io.parsing.Symbol.seq(com.avro.io.parsing.Symbol.repeat(com.avro.io.parsing.Symbol.MAP_END,
              com.avro.io.parsing.Symbol.ITEM_END, generate(sc.getValueType(), seen),
              com.avro.io.parsing.Symbol.MAP_KEY_MARKER, com.avro.io.parsing.Symbol.STRING),
          com.avro.io.parsing.Symbol.MAP_START);
    case RECORD: {
      ValidatingGrammarGenerator.LitS wsc = new ValidatingGrammarGenerator.LitS(sc);
      com.avro.io.parsing.Symbol rresult = seen.get(wsc);
      if (rresult == null) {
        com.avro.io.parsing.Symbol[] production = new com.avro.io.parsing.Symbol[sc.getFields().size() * 3 + 2];
        rresult = com.avro.io.parsing.Symbol.seq(production);
        seen.put(wsc, rresult);

        int i = production.length;
        int n = 0;
        production[--i] = com.avro.io.parsing.Symbol.RECORD_START;
        for (Field f : sc.getFields()) {
          production[--i] = com.avro.io.parsing.Symbol.fieldAdjustAction(n, f.name());
          production[--i] = generate(f.schema(), seen);
          production[--i] = com.avro.io.parsing.Symbol.FIELD_END;
          n++;
        }
        production[--i] = Symbol.RECORD_END;
      }
      return rresult;
    }
    default:
      throw new RuntimeException("Unexpected schema type");
    }
  }
}

