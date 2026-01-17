package io.dashbase.clue.commands;

import io.dashbase.clue.LuceneContext;
import io.dashbase.clue.api.BytesRefPrinter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.PrintStream;
import java.util.List;

@Readonly
@Command(name = "verifysort", mixinStandardHelpOptions = true)
public class VerifySortCommand extends ClueCommand {

  private final LuceneContext ctx;

  public VerifySortCommand(LuceneContext ctx) {
    super(ctx);
    this.ctx = ctx;
  }

  enum Order {
    asc,
    desc
  }

  enum Missing {
    fail,
    first,
    last
  }

  enum Selector {
    min,
    max
  }

  @Option(names = {"-f", "--field"}, required = true, description = "doc values field to verify sorting for")
  private String field;

  @Option(names = {"-o", "--order"}, defaultValue = "asc", description = "sort order: ${COMPLETION-CANDIDATES}")
  private Order order;

  @Option(names = {"--missing"}, defaultValue = "fail", description = "how to handle missing values: ${COMPLETION-CANDIDATES}")
  private Missing missing;

  @Option(names = {"--selector"}, defaultValue = "min", description = "selector for multi-valued doc values: ${COMPLETION-CANDIDATES}")
  private Selector selector;

  @Option(names = {"-v", "--verbose"}, defaultValue = "false", description = "print extra details")
  private boolean verbose;

  @Override
  public String getName() {
    return "verifysort";
  }

  @Override
  public String help() {
    return "verifies the index has exactly 1 segment and is sorted by <field> in docID order";
  }

  @Override
  public Integer call() throws Exception {
    if (ctx.isReadOnlyMode() && !getClass().isAnnotationPresent(Readonly.class)) {
      getOut().println("read-only mode, command: " + getName() + " is not allowed");
      return 1;
    }
    boolean ok = verify(getOut());
    return ok ? 0 : 1;
  }

  @Override
  protected void run(PrintStream out) throws Exception {
    verify(out);
  }

  private boolean verify(PrintStream out) throws Exception {
    IndexReader reader = ctx.getIndexReader();
    List<LeafReaderContext> leaves = reader.leaves();
    if (leaves.size() != 1) {
      out.println("FAIL: index must have exactly 1 segment; found " + leaves.size());
      out.println("Hint: run `merge -n 1` (not in readonly mode) to force-merge");
      return false;
    }

    LeafReader leaf = leaves.get(0).reader();
    if (verbose) {
      out.println("segment sort metadata: " + leaf.getMetaData().sort());
      out.println("maxDoc: " + leaf.maxDoc() + ", numDocs: " + leaf.numDocs() + ", deleted: " + leaf.numDeletedDocs());
    }

    FieldInfo fieldInfo = leaf.getFieldInfos().fieldInfo(field);
    if (fieldInfo == null) {
      out.println("FAIL: field not found: " + field);
      return false;
    }

    DocValuesType docValuesType = fieldInfo.getDocValuesType();
    if (docValuesType == null || docValuesType == DocValuesType.NONE) {
      out.println("FAIL: field has no doc values: " + field);
      return false;
    }

    boolean ok;
    switch (docValuesType) {
      case NUMERIC -> ok = verifyNumeric(leaf, out);
      case SORTED -> ok = verifySorted(leaf, out);
      case SORTED_NUMERIC -> ok = verifySortedNumeric(leaf, out);
      case SORTED_SET -> ok = verifySortedSet(leaf, out);
      case BINARY -> ok = verifyBinary(leaf, out);
      default -> {
        out.println("FAIL: unsupported doc values type for sorting verification: " + docValuesType);
        ok = false;
      }
    }

    if (ok) {
      out.println("OK: index is sorted by " + field + " " + order);
    }
    return ok;
  }

  private boolean verifyNumeric(LeafReader leaf, PrintStream out) throws Exception {
    Bits liveDocs = leaf.getLiveDocs();
    NumericDocValues values;
    try {
      values = DocValues.getNumeric(leaf, field);
    } catch (Exception e) {
      out.println("FAIL: unable to load numeric doc values for field: " + field + " (" + e.getMessage() + ")");
      return false;
    }

    int prevDocId = -1;
    boolean prevHas = false;
    long prevValue = 0L;

    for (int docId = 0; docId < leaf.maxDoc(); docId++) {
      if (liveDocs != null && !liveDocs.get(docId)) {
        continue;
      }

      boolean has = values.advanceExact(docId);
      if (!has && missing == Missing.fail) {
        out.println("FAIL: doc " + docId + " is missing doc values for field: " + field);
        return false;
      }
      long currentValue = has ? values.longValue() : 0L;

      if (prevDocId >= 0) {
        if (!checkMissingOrdering(prevHas, has)) {
          out.println("FAIL: missing value ordering violated at doc " + docId + " for field: " + field);
          return false;
        }
        if (prevHas && has && !checkOrder(prevValue, currentValue)) {
          out.println("FAIL: order violated at doc " + docId
              + " (prev doc " + prevDocId + " value=" + prevValue
              + ", current value=" + currentValue + ")");
          return false;
        }
      }

      prevDocId = docId;
      prevHas = has;
      prevValue = currentValue;
    }

    return true;
  }

  private boolean verifySorted(LeafReader leaf, PrintStream out) throws Exception {
    Bits liveDocs = leaf.getLiveDocs();
    SortedDocValues values;
    try {
      values = DocValues.getSorted(leaf, field);
    } catch (Exception e) {
      out.println("FAIL: unable to load sorted doc values for field: " + field + " (" + e.getMessage() + ")");
      return false;
    }

    int prevDocId = -1;
    boolean prevHas = false;
    int prevOrd = 0;

    for (int docId = 0; docId < leaf.maxDoc(); docId++) {
      if (liveDocs != null && !liveDocs.get(docId)) {
        continue;
      }

      boolean has = values.advanceExact(docId);
      if (!has && missing == Missing.fail) {
        out.println("FAIL: doc " + docId + " is missing doc values for field: " + field);
        return false;
      }
      int ord = has ? values.ordValue() : 0;

      if (prevDocId >= 0) {
        if (!checkMissingOrdering(prevHas, has)) {
          out.println("FAIL: missing value ordering violated at doc " + docId + " for field: " + field);
          return false;
        }
        if (prevHas && has && !checkOrder(prevOrd, ord)) {
          String prevValue = safeBytesToString(values.lookupOrd(prevOrd));
          String currentValue = safeBytesToString(values.lookupOrd(ord));
          out.println("FAIL: order violated at doc " + docId
              + " (prev doc " + prevDocId + " value=" + prevValue
              + ", current value=" + currentValue + ")");
          return false;
        }
      }

      prevDocId = docId;
      prevHas = has;
      prevOrd = ord;
    }

    return true;
  }

  private boolean verifySortedNumeric(LeafReader leaf, PrintStream out) throws Exception {
    Bits liveDocs = leaf.getLiveDocs();
    SortedNumericDocValues values;
    try {
      values = DocValues.getSortedNumeric(leaf, field);
    } catch (Exception e) {
      out.println("FAIL: unable to load sorted-numeric doc values for field: " + field + " (" + e.getMessage() + ")");
      return false;
    }

    int prevDocId = -1;
    boolean prevHas = false;
    long prevValue = 0L;

    for (int docId = 0; docId < leaf.maxDoc(); docId++) {
      if (liveDocs != null && !liveDocs.get(docId)) {
        continue;
      }

      boolean has = values.advanceExact(docId);
      if (!has && missing == Missing.fail) {
        out.println("FAIL: doc " + docId + " is missing doc values for field: " + field);
        return false;
      }
      long currentValue = has ? selectSortedNumeric(values) : 0L;

      if (prevDocId >= 0) {
        if (!checkMissingOrdering(prevHas, has)) {
          out.println("FAIL: missing value ordering violated at doc " + docId + " for field: " + field);
          return false;
        }
        if (prevHas && has && !checkOrder(prevValue, currentValue)) {
          out.println("FAIL: order violated at doc " + docId
              + " (prev doc " + prevDocId + " value=" + prevValue
              + ", current value=" + currentValue + ")");
          return false;
        }
      }

      prevDocId = docId;
      prevHas = has;
      prevValue = currentValue;
    }

    return true;
  }

  private boolean verifySortedSet(LeafReader leaf, PrintStream out) throws Exception {
    Bits liveDocs = leaf.getLiveDocs();
    SortedSetDocValues values;
    try {
      values = DocValues.getSortedSet(leaf, field);
    } catch (Exception e) {
      out.println("FAIL: unable to load sorted-set doc values for field: " + field + " (" + e.getMessage() + ")");
      return false;
    }

    int prevDocId = -1;
    boolean prevHas = false;
    long prevOrd = 0L;

    for (int docId = 0; docId < leaf.maxDoc(); docId++) {
      if (liveDocs != null && !liveDocs.get(docId)) {
        continue;
      }

      boolean has = values.advanceExact(docId);
      if (!has && missing == Missing.fail) {
        out.println("FAIL: doc " + docId + " is missing doc values for field: " + field);
        return false;
      }
      long ord = has ? selectSortedSet(values) : 0L;

      if (prevDocId >= 0) {
        if (!checkMissingOrdering(prevHas, has)) {
          out.println("FAIL: missing value ordering violated at doc " + docId + " for field: " + field);
          return false;
        }
        if (prevHas && has && !checkOrder(prevOrd, ord)) {
          String prevValue = safeBytesToString(values.lookupOrd(prevOrd));
          String currentValue = safeBytesToString(values.lookupOrd(ord));
          out.println("FAIL: order violated at doc " + docId
              + " (prev doc " + prevDocId + " value=" + prevValue
              + ", current value=" + currentValue + ")");
          return false;
        }
      }

      prevDocId = docId;
      prevHas = has;
      prevOrd = ord;
    }

    return true;
  }

  private boolean verifyBinary(LeafReader leaf, PrintStream out) throws Exception {
    Bits liveDocs = leaf.getLiveDocs();
    BinaryDocValues values;
    try {
      values = DocValues.getBinary(leaf, field);
    } catch (Exception e) {
      out.println("FAIL: unable to load binary doc values for field: " + field + " (" + e.getMessage() + ")");
      return false;
    }

    int prevDocId = -1;
    boolean prevHas = false;
    BytesRef prevValue = null;

    for (int docId = 0; docId < leaf.maxDoc(); docId++) {
      if (liveDocs != null && !liveDocs.get(docId)) {
        continue;
      }

      boolean has = values.advanceExact(docId);
      if (!has && missing == Missing.fail) {
        out.println("FAIL: doc " + docId + " is missing doc values for field: " + field);
        return false;
      }
      BytesRef currentValue = has ? BytesRef.deepCopyOf(values.binaryValue()) : null;

      if (prevDocId >= 0) {
        if (!checkMissingOrdering(prevHas, has)) {
          out.println("FAIL: missing value ordering violated at doc " + docId + " for field: " + field);
          return false;
        }
        if (prevHas && has) {
          int cmp = prevValue.compareTo(currentValue);
          if ((order == Order.asc && cmp > 0) || (order == Order.desc && cmp < 0)) {
            out.println("FAIL: order violated at doc " + docId + " (prev doc " + prevDocId + ")");
            if (verbose) {
              out.println("prev bytes length=" + prevValue.length + ", current bytes length=" + currentValue.length);
            }
            return false;
          }
        }
      }

      prevDocId = docId;
      prevHas = has;
      prevValue = currentValue;
    }

    return true;
  }

  private boolean checkMissingOrdering(boolean prevHas, boolean currentHas) {
    if (missing == Missing.fail) {
      return true;
    }
    if (!prevHas && !currentHas) {
      return true;
    }
    if (!prevHas && currentHas) {
      return missing == Missing.first;
    }
    if (prevHas && !currentHas) {
      return missing == Missing.last;
    }
    return true;
  }

  private boolean checkOrder(long prev, long current) {
    return order == Order.asc ? prev <= current : prev >= current;
  }

  private static String safeBytesToString(BytesRef bytesRef) {
    return BytesRefPrinter.toUtf8String(bytesRef);
  }

  private long selectSortedNumeric(SortedNumericDocValues values) throws Exception {
    int count = values.docValueCount();
    if (count <= 0) {
      return 0L;
    }
    long selected = values.nextValue();
    if (selector == Selector.max) {
      for (int i = 1; i < count; i++) {
        selected = values.nextValue();
      }
    }
    return selected;
  }

  private long selectSortedSet(SortedSetDocValues values) throws Exception {
    int count = values.docValueCount();
    if (count <= 0) {
      return 0L;
    }
    long selected = values.nextOrd();
    if (selector == Selector.max) {
      for (int i = 1; i < count; i++) {
        selected = values.nextOrd();
      }
    }
    return selected;
  }
}
