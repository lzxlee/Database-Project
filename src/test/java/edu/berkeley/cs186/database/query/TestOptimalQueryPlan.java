package edu.berkeley.cs186.database.query;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.StudentTestP4;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.table.MarkerRecord;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.StringHistogram;

import static org.junit.Assert.*;

public class TestOptimalQueryPlan {
  private Database database;
  private Random random = new Random();
  private String alphabet = StringHistogram.alphaNumeric;
  private String defaulTableName = "testAllTypes";
  private int defaultNumRecords = 1000;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws DatabaseException, IOException {
    File tempDir = tempFolder.newFolder("db");
    this.database = new Database(tempDir.getAbsolutePath());
    this.database.deleteAllTables();
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), this.defaulTableName);
    Database.Transaction transaction = this.database.beginTransaction();

    // by default, create 100 records
    for (int i = 0; i < this.defaultNumRecords; i++) {
      // generate a random record
      IntDataBox intValue = new IntDataBox(i);
      FloatDataBox floatValue = new FloatDataBox(this.random.nextFloat());
      BoolDataBox boolValue = new BoolDataBox(this.random.nextBoolean());
      String stringValue = "";

      for (int j = 0 ; j < 5; j++) {
        int randomIndex = Math.abs(this.random.nextInt() % alphabet.length());
        stringValue += alphabet.substring(randomIndex, randomIndex + 1);
      }

      List<DataBox> values = new ArrayList<DataBox>();
      values.add(boolValue);
      values.add(intValue);
      values.add(new StringDataBox(stringValue, 5));
      values.add(floatValue);

      transaction.addRecord("testAllTypes", values);
    }

    transaction.end();
  }

  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void testSample() {
    assertEquals(true, true); // Do not actually write a test like this!
  }

  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void test2() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);
    queryPlan.select("bool", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new BoolDataBox(true));
    queryPlan.executeOptimal();
    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
            "column: testAllTypes.bool\n" +
            "operator: GREATER_THAN_EQUALS\n" +
            "value: true\n" +
            "\ttype: SEQSCAN\n" +
            "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());
    transaction.end();
  }

  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void test3() throws QueryPlanException, DatabaseException {
    List<DataBox> values = TestUtils.createRecordWithAllTypes().getValues();
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), "t1");
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), "t2");
    Database.Transaction transaction = this.database.beginTransaction();
    for (int i = 0; i < 10000; i++) {
      transaction.addRecord("t1", values);
    }
    for (int j = 0; j < 20000; j++) {
      transaction.addRecord("t2", values);
    }
    QueryOperator left = new SequentialScanOperator(transaction, "t1");
    QueryOperator right = new SequentialScanOperator(transaction, "t2");
    JoinOperator joined = new GraceHashOperator(left, right, "string", "string", transaction);
    assertEquals(315, joined.estimateIOCost());
  }
  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void test4() throws QueryPlanException, DatabaseException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);
    queryPlan.groupBy("string");
    queryPlan.select("string", QueryPlan.PredicateOperator.EQUALS, new StringDataBox(2));
    Iterator<Record> recordIterator = queryPlan.execute();
    int count = 0;
    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      count ++;
    }
    assertEquals(0, count);
    transaction.end();
  }

  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void test5() throws DatabaseException, QueryPlanException, java.io.IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new BNLJOperator(sourceOperator, sourceOperator, "bool", "bool", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    int count = 0;
    while (outputIterator.hasNext()) {
      count++;
      outputIterator.next();
    }
    assertEquals(10000, count);
  }
  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void test6() throws DatabaseException, QueryPlanException {
    List<DataBox> values = TestUtils.createRecordWithAllTypes().getValues();
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), "t3");
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), "t4");
    Database.Transaction transaction = this.database.beginTransaction();
    for (int i = 0; i < 8000; i++) {
      transaction.addRecord("t3", values);
    }
    for (int j = 0; j < 10000; j++) {
      transaction.addRecord("t4", values);
    }
    QueryOperator left = new SequentialScanOperator(transaction, "t3");
    QueryOperator right = new SequentialScanOperator(transaction, "t4");
    JoinOperator joined = new SNLJOperator(left, right, "float", "float", transaction);
    JoinOperator j2 = new BNLJOperator(left, right, "bool", "bool", transaction);
    JoinOperator j3 = new GraceHashOperator(left, right, "int", "int", transaction);
    assertTrue(j2.estimateIOCost()<joined.estimateIOCost());
    assertTrue(j3.estimateIOCost() < joined.estimateIOCost());
    assertEquals(378, j2.estimateIOCost());
  }
  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void test7() throws DatabaseException, QueryPlanException, java.io.IOException {
    TestSourceOperator sourceOperator = new TestSourceOperator();
    File tempDir = tempFolder.newFolder("joinTest");
    Database.Transaction transaction = new Database(tempDir.getAbsolutePath()).beginTransaction();
    JoinOperator joinOperator = new BNLJOperator(sourceOperator, sourceOperator, "string", "string", transaction);
    Iterator<Record> outputIterator = joinOperator.iterator();
    assertTrue(outputIterator.hasNext());
  }
  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void test8() throws QueryPlanException, DatabaseException {
    List<DataBox> values = TestUtils.createRecordWithAllTypes().getValues();
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), "t3");
    this.database.createTable(TestUtils.createSchemaWithAllTypes(), "t4");
    Database.Transaction transaction = this.database.beginTransaction();
    for (int i = 0; i < 9; i++) {
      transaction.addRecord("t3", values);
    }
    for (int j = 0; j < 99999; j++) {
      transaction.addRecord("t4", values);
    }
    QueryOperator left = new SequentialScanOperator(transaction, "t3");
    QueryOperator right = new SequentialScanOperator(transaction, "t4");
    JoinOperator joined = new SNLJOperator(left, right, "bool", "bool", transaction);
    assertEquals(3133, joined.estimateIOCost());
  }
  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void test9() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);
    queryPlan.select("int", QueryPlan.PredicateOperator.EQUALS, new IntDataBox());
    queryPlan.executeOptimal();
    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
            "column: testAllTypes.int\n" +
            "operator: EQUALS\n" +
            "value: 0\n" +
            "\ttype: SEQSCAN\n" +
            "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());
    transaction.end();
  }

  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP4.class)
  public void test10() {
    assertEquals(true, true); // Do not actually write a test like this!
  }

  @Test(timeout=1000)
  public void testSimpleProjectIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("int");
    columnNames.add("string");

    queryPlan.project(columnNames);
    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    int count = 0;
    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();
      assertTrue(record.getValues().get(0) instanceof IntDataBox);
      assertTrue(record.getValues().get(1) instanceof StringDataBox);

      count++;
    }

    assertEquals(this.defaultNumRecords, count);

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: PROJECT\n" +
                  "columns: [int, string]\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=1000)
  public void testSimpleSelectIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.select("int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(0));

    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();

      assertTrue(record.getValues().get(1).getInt() >= 0);
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: SELECT\n" +
                  "column: testAllTypes.int\n" +
                  "operator: GREATER_THAN_EQUALS\n" +
                  "value: 0\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=60000)
  public void testSimpleGroupByIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);
    MarkerRecord markerRecord = MarkerRecord.getMarker();

    queryPlan.groupBy("int");
    Iterator<Record> outputIterator = queryPlan.executeOptimal();

    boolean first = true;
    int prevValue = 0;
    while (outputIterator.hasNext()) {
      Record record = outputIterator.next();

      if (first) {
        prevValue = record.getValues().get(1).getInt();
        first = false;
      } else if (record == markerRecord) {
        first = true;
      } else {
        assertEquals(prevValue, record.getValues().get(1).getInt());
      }
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: GROUPBY\n" +
                  "column: testAllTypes.int\n" +
                  "\ttype: SEQSCAN\n" +
                  "\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

  @Test(timeout=1000)
  public void testProjectSelectIterator() throws DatabaseException, QueryPlanException {
    Database.Transaction transaction = this.database.beginTransaction();
    QueryPlan queryPlan = transaction.query(this.defaulTableName);

    queryPlan.select("int", QueryPlan.PredicateOperator.GREATER_THAN_EQUALS, new IntDataBox(0));

    List<String> columnNames = new ArrayList<String>();
    columnNames.add("bool");
    columnNames.add("int");
    queryPlan.project(columnNames);

    Iterator<Record> recordIterator = queryPlan.executeOptimal();

    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      List<DataBox> values = record.getValues();

      assertEquals(2, values.size());
      assertTrue(values.get(0) instanceof BoolDataBox);
      assertTrue(values.get(1) instanceof IntDataBox);

      assertTrue(values.get(1).getInt() >= 0);
    }

    QueryOperator finalOperator = queryPlan.getFinalOperator();
    String tree = "type: PROJECT\n" +
                  "columns: [bool, int]\n" +
                  "\ttype: SELECT\n" +
                  "\tcolumn: testAllTypes.int\n" +
                  "\toperator: GREATER_THAN_EQUALS\n" +
                  "\tvalue: 0\n" +
                  "\t\ttype: SEQSCAN\n" +
                  "\t\ttable: testAllTypes";
    assertEquals(tree, finalOperator.toString());

    transaction.end();
  }

}
