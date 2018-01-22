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

import java.util.*;

/**
 * Symbol is the base of all symbols (terminals and non-terminals) of
 * the grammar.
 */
public abstract class Symbol {
  /*
   * The type of symbol.
   */
  public enum Kind {
    /** terminal symbols which have no productions */
    TERMINAL,
    /** Start symbol for some grammar */
    ROOT,
    /** non-terminal symbol which is a sequence of one or more other symbols */
    SEQUENCE,
    /** non-termial to represent the contents of an array or map */
    REPEATER,
    /** non-terminal to represent the union */
    ALTERNATIVE,
    /** non-terminal action symbol which are automatically consumed */
    IMPLICIT_ACTION,
    /** non-terminal action symbol which is explicitly consumed */
    EXPLICIT_ACTION
  };

  /// The kind of this symbol.
  public final Kind kind;

  /**
   * The production for this symbol. If this symbol is a terminal
   * this is <tt>null</tt>. Otherwise this holds the the sequence of
   * the symbols that forms the production for this symbol. The
   * sequence is in the reverse order of production. This is useful
   * for easy copying onto parsing stack.
   *
   * Please note that this is a final. So the production for a symbol
   * should be known before that symbol is constructed. This requirement
   * cannot be met for those symbols which are recursive (e.g. a record that
   * holds union a branch of which is the record itself). To resolve this
   * problem, we initialize the symbol with an array of nulls. Later we
   * fill the symbols. Not clean, but works. The other option is to not have
   * this field a final. But keeping it final and thus keeping symbol immutable
   * gives some comfort. See various generators how we generate records.
   */
  public final com.avro.io.parsing.Symbol[] production;
  /**
   * Constructs a new symbol of the given kind <tt>kind</tt>.
   */
  protected Symbol(Kind kind) {
    this(kind, null);
  }


  protected Symbol(Kind kind, com.avro.io.parsing.Symbol[] production) {
    this.production = production;
    this.kind = kind;
  }

  /**
   * A convenience method to construct a root symbol.
   */
  static com.avro.io.parsing.Symbol root(com.avro.io.parsing.Symbol... symbols) {
    return new Root(symbols);
  }
  /**
   * A convenience method to construct a sequence.
   * @param production  The constituent symbols of the sequence.
   */
  static com.avro.io.parsing.Symbol seq(com.avro.io.parsing.Symbol... production) {
    return new Sequence(production);
  }

  /**
   * A convenience method to construct a repeater.
   * @param symsToRepeat The symbols to repeat in the repeater.
   */
  static com.avro.io.parsing.Symbol repeat(com.avro.io.parsing.Symbol endSymbol, com.avro.io.parsing.Symbol... symsToRepeat) {
    return new Repeater(endSymbol, symsToRepeat);
  }

  /**
   *  A convenience method to construct a union.
   */
  static com.avro.io.parsing.Symbol alt(com.avro.io.parsing.Symbol[] symbols, String[] labels) {
    return new Alternative(symbols, labels);
  }

  /**
   * A convenience method to construct an ErrorAction.
   * @param e
   */
  static com.avro.io.parsing.Symbol error(String e) {
    return new ErrorAction(e);
  }

  /**
   * A convenience method to construct a ResolvingAction.
   * @param w The writer symbol
   * @param r The reader symbol
   */
  static com.avro.io.parsing.Symbol resolve(com.avro.io.parsing.Symbol w, com.avro.io.parsing.Symbol r) {
    return new ResolvingAction(w, r);
  }

  private static class Fixup {
    public final com.avro.io.parsing.Symbol[] symbols;
    public final int pos;

    public Fixup(com.avro.io.parsing.Symbol[] symbols, int pos) {
      this.symbols = symbols;
      this.pos = pos;
    }
  }

  public com.avro.io.parsing.Symbol flatten(Map<Sequence, Sequence> map,
                                            Map<Sequence, List<Fixup>> map2) {
    return this;
  }

  public int flattenedSize() {
    return 1;
  }

  /**
   * Flattens the given sub-array of symbols into an sub-array of symbols. Every
   * <tt>Sequence</tt> in the input are replaced by its production recursively.
   * Non-<tt>Sequence</tt> symbols, they internally have other symbols
   * those internal symbols also get flattened.
   *
   * The algorithm does a few tricks to handle recursive symbol definitions.
   * In order to avoid infinite recursion with recursive symbols, we have a map
   * of Symbol->Symbol. Before fully constructing a flattened symbol for a
   * <tt>Sequence</tt> we insert an empty output symbol into the map and then
   * start filling the production for the <tt>Sequence</tt>. If the same
   * <tt>Sequence</tt> is encountered due to recursion, we simply return the
   * (empty) output <tt>Sequence<tt> from the map. Then we actually fill out
   * the production for the <tt>Sequence</tt>.
   * As part of the flattening process we copy the production of
   * <tt>Sequence</tt>s into larger arrays. If the original <tt>Sequence</tt>
   * has not not be fully constructed yet, we copy a bunch of <tt>null</tt>s.
   * Fix-up remembers all those <tt>null</tt> patches. The fix-ups gets finally
   * filled when we know the symbols to occupy those patches.
   *
   * @param in  The array of input symbols to flatten
   * @param start The position where the input sub-array starts.
   * @param out The output that receives the flattened list of symbols. The
   * output array should have sufficient space to receive the expanded sub-array
   * of symbols.
   * @param skip  The position where the output input sub-array starts.
   * @param map A map of symbols which have already been expanded. Useful for
   * handling recursive definitions and for caching.
   * @param map2  A map to to store the list of fix-ups.
   */
  static void flatten(com.avro.io.parsing.Symbol[] in, int start,
                      com.avro.io.parsing.Symbol[] out, int skip,
                      Map<Sequence, Sequence> map,
                      Map<Sequence, List<Fixup>> map2) {
    for (int i = start, j = skip; i < in.length; i++) {
      com.avro.io.parsing.Symbol s = in[i].flatten(map, map2);
      if (s instanceof Sequence) {
        com.avro.io.parsing.Symbol[] p = s.production;
        List<Fixup> l = map2.get(s);
        if (l == null) {
          System.arraycopy(p, 0, out, j, p.length);
          // Copy any fixups that will be applied to p to add missing symbols
          for (List<Fixup> fixups : map2.values()) {
            copyFixups(fixups, out, j, p);
          }
        } else {
          l.add(new Fixup(out, j));
        }
        j += p.length;
      } else {
        out[j++] = s;
      }
    }
  }

  private static void copyFixups(List<Fixup> fixups, com.avro.io.parsing.Symbol[] out, int outPos,
                                 com.avro.io.parsing.Symbol[] toCopy) {
    for (int i = 0, n = fixups.size(); i < n; i += 1) {
      Fixup fixup = fixups.get(i);
      if (fixup.symbols == toCopy) {
        fixups.add(new Fixup(out, fixup.pos + outPos));
      }
    }
  }

  /**
   * Returns the amount of space required to flatten the given
   * sub-array of symbols.
   * @param symbols The array of input symbols.
   * @param start The index where the subarray starts.
   * @return  The number of symbols that will be produced if one expands
   * the given input.
   */
  protected static int flattenedSize(com.avro.io.parsing.Symbol[] symbols, int start) {
    int result = 0;
    for (int i = start; i < symbols.length; i++) {
      if (symbols[i] instanceof Sequence) {
        Sequence s = (Sequence) symbols[i];
        result += s.flattenedSize();
      } else {
        result += 1;
      }
    }
    return result;
  }

  private static class Terminal extends com.avro.io.parsing.Symbol {
    private final String printName;
    public Terminal(String printName) {
      super(Kind.TERMINAL);
      this.printName = printName;
    }
    public String toString() { return printName; }
  }

  public static class ImplicitAction extends com.avro.io.parsing.Symbol {
    /**
     * Set to <tt>true</tt> if and only if this implicit action is
     * a trailing action. That is, it is an action that follows
     * real symbol. E.g {@link com.avro.io.parsing.Symbol#DEFAULT_END_ACTION}.
     */
    public final boolean isTrailing;

    private ImplicitAction() {
      this(false);
    }

    private ImplicitAction(boolean isTrailing) {
      super(Kind.IMPLICIT_ACTION);
      this.isTrailing = isTrailing;
    }
  }

  protected static class Root extends com.avro.io.parsing.Symbol {
    private Root(com.avro.io.parsing.Symbol... symbols) {
      super(Kind.ROOT, makeProduction(symbols));
      production[0] = this;
    }

    private static com.avro.io.parsing.Symbol[] makeProduction(com.avro.io.parsing.Symbol[] symbols) {
      com.avro.io.parsing.Symbol[] result = new com.avro.io.parsing.Symbol[flattenedSize(symbols, 0) + 1];
      flatten(symbols, 0, result, 1,
          new HashMap<Sequence, Sequence>(),
          new HashMap<Sequence, List<Fixup>>());
      return result;
    }
  }

  protected static class Sequence extends com.avro.io.parsing.Symbol implements Iterable<com.avro.io.parsing.Symbol> {
    private Sequence(com.avro.io.parsing.Symbol[] productions) {
      super(Kind.SEQUENCE, productions);
    }

    public com.avro.io.parsing.Symbol get(int index) {
      return production[index];
    }

    public int size() {
      return production.length;
    }

    public Iterator<com.avro.io.parsing.Symbol> iterator() {
      return new Iterator<com.avro.io.parsing.Symbol>() {
        private int pos = production.length;

        public boolean hasNext() {
          return 0 < pos;
        }

        public com.avro.io.parsing.Symbol next() {
          if (0 < pos) {
            return production[--pos];
          } else {
            throw new NoSuchElementException();
          }
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
    @Override
    public Sequence flatten(Map<Sequence, Sequence> map,
                            Map<Sequence, List<Fixup>> map2) {
      Sequence result = map.get(this);
      if (result == null) {
        result = new Sequence(new com.avro.io.parsing.Symbol[flattenedSize()]);
        map.put(this, result);
        List<Fixup> l = new ArrayList<Fixup>();
        map2.put(result, l);

        flatten(production, 0,
            result.production, 0, map, map2);
        for (Fixup f : l) {
          System.arraycopy(result.production, 0, f.symbols, f.pos,
              result.production.length);
        }
        map2.remove(result);
      }
      return result;
    }

    @Override
    public final int flattenedSize() {
      return flattenedSize(production, 0);
    }
  }

  public static class Repeater extends com.avro.io.parsing.Symbol {
    public final com.avro.io.parsing.Symbol end;

    private Repeater(com.avro.io.parsing.Symbol end, com.avro.io.parsing.Symbol... sequenceToRepeat) {
      super(Kind.REPEATER, makeProduction(sequenceToRepeat));
      this.end = end;
      production[0] = this;
    }

    private static com.avro.io.parsing.Symbol[] makeProduction(com.avro.io.parsing.Symbol[] p) {
      com.avro.io.parsing.Symbol[] result = new com.avro.io.parsing.Symbol[p.length + 1];
      System.arraycopy(p, 0, result, 1, p.length);
      return result;
    }

    @Override
    public Repeater flatten(Map<Sequence, Sequence> map,
                            Map<Sequence, List<Fixup>> map2) {
      Repeater result =
        new Repeater(end, new com.avro.io.parsing.Symbol[flattenedSize(production, 1)]);
      flatten(production, 1, result.production, 1, map, map2);
      return result;
    }

  }

  /**
   * Returns true if the Parser contains any Error symbol, indicating that it may fail
   * for some inputs.
   */
  public static boolean hasErrors(com.avro.io.parsing.Symbol symbol) {
    switch(symbol.kind) {
    case ALTERNATIVE:
      return hasErrors(symbol, ((Alternative) symbol).symbols);
    case EXPLICIT_ACTION:
      return false;
    case IMPLICIT_ACTION:
      return symbol instanceof ErrorAction;
    case REPEATER:
      Repeater r = (Repeater) symbol;
      return hasErrors(r.end) || hasErrors(symbol, r.production);
    case ROOT:
    case SEQUENCE:
      return hasErrors(symbol, symbol.production);
    case TERMINAL:
      return false;
    default:
      throw new RuntimeException("unknown symbol kind: " + symbol.kind);
    }
  }

  private static boolean hasErrors(com.avro.io.parsing.Symbol root, com.avro.io.parsing.Symbol[] symbols) {
    if(null != symbols) {
      for(com.avro.io.parsing.Symbol s: symbols) {
        if (s == root) {
          continue;
        }
        if (hasErrors(s)) {
          return true;
        }
      }
    }
    return false;
  }

  public static class Alternative extends com.avro.io.parsing.Symbol {
    public final com.avro.io.parsing.Symbol[] symbols;
    public final String[] labels;
    private Alternative(com.avro.io.parsing.Symbol[] symbols, String[] labels) {
      super(Kind.ALTERNATIVE);
      this.symbols = symbols;
      this.labels = labels;
    }

    public com.avro.io.parsing.Symbol getSymbol(int index) {
      return symbols[index];
    }

    public String getLabel(int index) {
      return labels[index];
    }

    public int size() {
      return symbols.length;
    }

    public int findLabel(String label) {
      if (label != null) {
        for (int i = 0; i < labels.length; i++) {
          if (label.equals(labels[i])) {
            return i;
          }
        }
      }
      return -1;
    }

    @Override
    public Alternative flatten(Map<Sequence, Sequence> map,
                               Map<Sequence, List<Fixup>> map2) {
      com.avro.io.parsing.Symbol[] ss = new com.avro.io.parsing.Symbol[symbols.length];
      for (int i = 0; i < ss.length; i++) {
        ss[i] = symbols[i].flatten(map, map2);
      }
      return new Alternative(ss, labels);
    }
  }

  public static class ErrorAction extends ImplicitAction {
    public final String msg;
    private ErrorAction(String msg) {
      this.msg = msg;
    }
  }

  public static IntCheckAction intCheckAction(int size) {
    return new IntCheckAction(size);
  }

  public static class IntCheckAction extends com.avro.io.parsing.Symbol {
    public final int size;
    @Deprecated public IntCheckAction(int size) {
      super(Kind.EXPLICIT_ACTION);
      this.size = size;
    }
  }

  public static EnumAdjustAction enumAdjustAction(int rsymCount, Object[] adj) {
    return new EnumAdjustAction(rsymCount, adj);
  }

  public static class EnumAdjustAction extends IntCheckAction {
    public final Object[] adjustments;
    @Deprecated public EnumAdjustAction(int rsymCount, Object[] adjustments) {
      super(rsymCount);
      this.adjustments = adjustments;
    }
  }

  public static WriterUnionAction writerUnionAction() {
    return new WriterUnionAction();
  }

  public static class WriterUnionAction extends ImplicitAction {
    private WriterUnionAction() {}
  }

  public static class ResolvingAction extends ImplicitAction {
    public final com.avro.io.parsing.Symbol writer;
    public final com.avro.io.parsing.Symbol reader;
    private ResolvingAction(com.avro.io.parsing.Symbol writer, com.avro.io.parsing.Symbol reader) {
      this.writer = writer;
      this.reader = reader;
    }

    @Override
    public ResolvingAction flatten(Map<Sequence, Sequence> map,
                                   Map<Sequence, List<Fixup>> map2) {
      return new ResolvingAction(writer.flatten(map, map2),
          reader.flatten(map, map2));
    }

  }

  public static SkipAction skipAction(com.avro.io.parsing.Symbol symToSkip) {
    return new SkipAction(symToSkip);
  }

  public static class SkipAction extends ImplicitAction {
    public final com.avro.io.parsing.Symbol symToSkip;
    @Deprecated public SkipAction(com.avro.io.parsing.Symbol symToSkip) {
      super(true);
      this.symToSkip = symToSkip;
    }

    @Override
    public SkipAction flatten(Map<Sequence, Sequence> map,
                              Map<Sequence, List<Fixup>> map2) {
      return new SkipAction(symToSkip.flatten(map, map2));
    }

  }

  public static FieldAdjustAction fieldAdjustAction(int rindex, String fname) {
    return new FieldAdjustAction(rindex, fname);
  }

  public static class FieldAdjustAction extends ImplicitAction {
    public final int rindex;
    public final String fname;
    @Deprecated public FieldAdjustAction(int rindex, String fname) {
      this.rindex = rindex;
      this.fname = fname;
    }
  }

  public static FieldOrderAction fieldOrderAction(Schema.Field[] fields) {
    return new FieldOrderAction(fields);
  }

  public static final class FieldOrderAction extends ImplicitAction {
    public final Schema.Field[] fields;
    @Deprecated public FieldOrderAction(Schema.Field[] fields) {
      this.fields = fields;
    }
  }

  public static DefaultStartAction defaultStartAction(byte[] contents) {
    return new DefaultStartAction(contents);
  }

  public static class DefaultStartAction extends ImplicitAction {
    public final byte[] contents;
    @Deprecated public DefaultStartAction(byte[] contents) {
      this.contents = contents;
    }
  }

  public static UnionAdjustAction unionAdjustAction(int rindex, com.avro.io.parsing.Symbol sym) {
    return new UnionAdjustAction(rindex, sym);
  }

  public static class UnionAdjustAction extends ImplicitAction {
    public final int rindex;
    public final com.avro.io.parsing.Symbol symToParse;
    @Deprecated public UnionAdjustAction(int rindex, com.avro.io.parsing.Symbol symToParse) {
      this.rindex = rindex;
      this.symToParse = symToParse;
    }

    @Override
    public UnionAdjustAction flatten(Map<Sequence, Sequence> map,
                                     Map<Sequence, List<Fixup>> map2) {
      return new UnionAdjustAction(rindex, symToParse.flatten(map, map2));
    }

  }

  /** For JSON. */
  public static EnumLabelsAction enumLabelsAction(List<String> symbols) {
    return new EnumLabelsAction(symbols);
  }

  public static class EnumLabelsAction extends IntCheckAction {
    public final List<String> symbols;
    @Deprecated public EnumLabelsAction(List<String> symbols) {
      super(symbols.size());
      this.symbols = symbols;
    }

    public String getLabel(int n) {
      return symbols.get(n);
    }

    public int findLabel(String l) {
      if (l != null) {
        for (int i = 0; i < symbols.size(); i++) {
          if (l.equals(symbols.get(i))) {
            return i;
          }
        }
      }
      return -1;
    }
  }

  /**
   * The terminal symbols for the grammar.
   */
  public static final com.avro.io.parsing.Symbol NULL = new com.avro.io.parsing.Symbol.Terminal("null");
  public static final com.avro.io.parsing.Symbol BOOLEAN = new com.avro.io.parsing.Symbol.Terminal("boolean");
  public static final com.avro.io.parsing.Symbol INT = new com.avro.io.parsing.Symbol.Terminal("int");
  public static final com.avro.io.parsing.Symbol LONG = new com.avro.io.parsing.Symbol.Terminal("long");
  public static final com.avro.io.parsing.Symbol FLOAT = new com.avro.io.parsing.Symbol.Terminal("float");
  public static final com.avro.io.parsing.Symbol DOUBLE = new com.avro.io.parsing.Symbol.Terminal("double");
  public static final com.avro.io.parsing.Symbol STRING = new com.avro.io.parsing.Symbol.Terminal("string");
  public static final com.avro.io.parsing.Symbol BYTES = new com.avro.io.parsing.Symbol.Terminal("bytes");
  public static final com.avro.io.parsing.Symbol FIXED = new com.avro.io.parsing.Symbol.Terminal("fixed");
  public static final com.avro.io.parsing.Symbol ENUM = new com.avro.io.parsing.Symbol.Terminal("enum");
  public static final com.avro.io.parsing.Symbol UNION = new com.avro.io.parsing.Symbol.Terminal("union");

  public static final com.avro.io.parsing.Symbol ARRAY_START = new com.avro.io.parsing.Symbol.Terminal("array-start");
  public static final com.avro.io.parsing.Symbol ARRAY_END = new com.avro.io.parsing.Symbol.Terminal("array-end");
  public static final com.avro.io.parsing.Symbol MAP_START = new com.avro.io.parsing.Symbol.Terminal("map-start");
  public static final com.avro.io.parsing.Symbol MAP_END = new com.avro.io.parsing.Symbol.Terminal("map-end");
  public static final com.avro.io.parsing.Symbol ITEM_END = new com.avro.io.parsing.Symbol.Terminal("item-end");

  /* a pseudo terminal used by parsers */
  public static final com.avro.io.parsing.Symbol FIELD_ACTION =
    new com.avro.io.parsing.Symbol.Terminal("field-action");

  public static final com.avro.io.parsing.Symbol RECORD_START = new ImplicitAction(false);
  public static final com.avro.io.parsing.Symbol RECORD_END = new ImplicitAction(true);
  public static final com.avro.io.parsing.Symbol UNION_END = new ImplicitAction(true);
  public static final com.avro.io.parsing.Symbol FIELD_END = new ImplicitAction(true);

  public static final com.avro.io.parsing.Symbol DEFAULT_END_ACTION = new ImplicitAction(true);
  public static final com.avro.io.parsing.Symbol MAP_KEY_MARKER =
    new com.avro.io.parsing.Symbol.Terminal("map-key-marker");
}

