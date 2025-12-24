package io.dashbase.clue.commands;

import io.dashbase.clue.ClueApplication;
import io.dashbase.clue.LuceneContext;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VerifySortCommandTest {

  @Test
  void requiresSingleSegment(@TempDir Path tempDir) throws Exception {
    Path indexDir = buildMultiSegmentYearIndex(tempDir.resolve("multi"), new long[]{2001, 1999});
    LuceneContext ctx = CommandTestSupport.newContext(indexDir);
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      int code;
      try (PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
        code = ClueApplication.handleCommand(ctx, "verifysort", new String[]{"-f", "year"}, out);
      }
      String output = buffer.toString(StandardCharsets.UTF_8);
      assertEquals(1, code);
      assertTrue(output.contains("exactly 1 segment"));
    } finally {
      CommandTestSupport.shutdown(ctx);
    }
  }

  @Test
  void passesForSortedNumericAsc(@TempDir Path tempDir) throws Exception {
    Path indexDir = buildYearIndex(tempDir.resolve("sorted"), new long[]{1999, 2001, 2001, 2020});
    LuceneContext ctx = CommandTestSupport.newContext(indexDir);
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      int code;
      try (PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
        code = ClueApplication.handleCommand(ctx, "verifysort", new String[]{"-f", "year", "-o", "asc"}, out);
      }
      String output = buffer.toString(StandardCharsets.UTF_8);
      assertEquals(0, code);
      assertTrue(output.contains("OK: index is sorted by year asc"));
    } finally {
      CommandTestSupport.shutdown(ctx);
    }
  }

  @Test
  void failsForUnsortedNumericAsc(@TempDir Path tempDir) throws Exception {
    Path indexDir = buildYearIndex(tempDir.resolve("unsorted"), new long[]{2001, 1999, 2020});
    LuceneContext ctx = CommandTestSupport.newContext(indexDir);
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      int code;
      try (PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
        code = ClueApplication.handleCommand(ctx, "verifysort", new String[]{"-f", "year", "-o", "asc"}, out);
      }
      String output = buffer.toString(StandardCharsets.UTF_8);
      assertEquals(1, code);
      assertTrue(output.contains("FAIL: order violated"));
    } finally {
      CommandTestSupport.shutdown(ctx);
    }
  }

  private static Path buildYearIndex(Path indexDir, long[] years) throws Exception {
    Files.createDirectories(indexDir);
    try (Directory dir = FSDirectory.open(indexDir)) {
      IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (long year : years) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("year", year));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.commit();
      }
    }
    return indexDir;
  }

  private static Path buildMultiSegmentYearIndex(Path indexDir, long[] years) throws Exception {
    Files.createDirectories(indexDir);
    try (Directory dir = FSDirectory.open(indexDir)) {
      IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer())
          .setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (long year : years) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("year", year));
          writer.addDocument(doc);
          writer.commit();
        }
      }
    }
    return indexDir;
  }
}
