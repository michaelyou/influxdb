package inmem

import (
	"fmt"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// Test comparing SeriesIDs for equality.
func TestSeriesIDs_Equals(t *testing.T) {
	ids1 := seriesIDs([]uint64{1, 2, 3})
	ids2 := seriesIDs([]uint64{1, 2, 3})
	ids3 := seriesIDs([]uint64{4, 5, 6})

	if !ids1.equals(ids2) {
		t.Fatal("expected ids1 == ids2")
	} else if ids1.equals(ids3) {
		t.Fatal("expected ids1 != ids3")
	}
}

// Test intersecting sets of SeriesIDs.
func TestSeriesIDs_Intersect(t *testing.T) {
	// Test swapping l & r, all branches of if-else, and exit loop when 'j < len(r)'
	ids1 := seriesIDs([]uint64{1, 3, 4, 5, 6})
	ids2 := seriesIDs([]uint64{1, 2, 3, 7})
	exp := seriesIDs([]uint64{1, 3})
	got := ids1.intersect(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}

	// Test exit for loop when 'i < len(l)'
	ids1 = seriesIDs([]uint64{1})
	ids2 = seriesIDs([]uint64{1, 2})
	exp = seriesIDs([]uint64{1})
	got = ids1.intersect(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}
}

// Test union sets of SeriesIDs.
func TestSeriesIDs_Union(t *testing.T) {
	// Test all branches of if-else, exit loop because of 'j < len(r)', and append remainder from left.
	ids1 := seriesIDs([]uint64{1, 2, 3, 7})
	ids2 := seriesIDs([]uint64{1, 3, 4, 5, 6})
	exp := seriesIDs([]uint64{1, 2, 3, 4, 5, 6, 7})
	got := ids1.union(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}

	// Test exit because of 'i < len(l)' and append remainder from right.
	ids1 = seriesIDs([]uint64{1})
	ids2 = seriesIDs([]uint64{1, 2})
	exp = seriesIDs([]uint64{1, 2})
	got = ids1.union(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}
}

// Test removing one set of SeriesIDs from another.
func TestSeriesIDs_Reject(t *testing.T) {
	// Test all branches of if-else, exit loop because of 'j < len(r)', and append remainder from left.
	ids1 := seriesIDs([]uint64{1, 2, 3, 7})
	ids2 := seriesIDs([]uint64{1, 3, 4, 5, 6})
	exp := seriesIDs([]uint64{2, 7})
	got := ids1.reject(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}

	// Test exit because of 'i < len(l)'.
	ids1 = seriesIDs([]uint64{1})
	ids2 = seriesIDs([]uint64{1, 2})
	exp = seriesIDs{}
	got = ids1.reject(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}
}

func TestMeasurement_AddSeries_Nil(t *testing.T) {
	m := newMeasurement("foo", "cpu")
	if m.addSeries(nil) {
		t.Fatalf("addSeries mismatch: exp false, got true")
	}
}

func TestMeasurement_AppendSeriesKeysByID_Missing(t *testing.T) {
	m := newMeasurement("foo", "cpu")
	var dst []string
	dst = m.appendSeriesKeysByID(dst, []uint64{1})
	if exp, got := 0, len(dst); exp != got {
		t.Fatalf("series len mismatch: exp %v, got %v", exp, got)
	}
}

func TestMeasurement_AppendSeriesKeysByID_Exists(t *testing.T) {
	m := newMeasurement("foo", "cpu")
	s := newSeries(1, m, "cpu,host=foo", models.Tags{models.NewTag([]byte("host"), []byte("foo"))})
	m.addSeries(s)

	var dst []string
	dst = m.appendSeriesKeysByID(dst, []uint64{1})
	if exp, got := 1, len(dst); exp != got {
		t.Fatalf("series len mismatch: exp %v, got %v", exp, got)
	}

	if exp, got := "cpu,host=foo", dst[0]; exp != got {
		t.Fatalf("series mismatch: exp %v, got %v", exp, got)
	}
}

func TestMeasurement_TagsSet_Deadlock(t *testing.T) {
	m := newMeasurement("foo", "cpu")
	s1 := newSeries(1, m, "cpu,host=foo", models.Tags{models.NewTag([]byte("host"), []byte("foo"))})
	m.addSeries(s1)

	s2 := newSeries(2, m, "cpu,host=bar", models.Tags{models.NewTag([]byte("host"), []byte("bar"))})
	m.addSeries(s2)

	m.dropSeries(s1)

	// This was deadlocking
	s := tsdb.NewSeriesIDSet()
	s.Add(1)
	m.tagSets(s, query.IteratorOptions{})
	if got, exp := len(m.seriesIDs()), 1; got != exp {
		t.Fatalf("series count mismatch: got %v, exp %v", got, exp)
	}
}

func BenchmarkMeasurement_SeriesIDForExp_EQRegex(b *testing.B) {
	m := newMeasurement("foo", "cpu")
	for i := 0; i < 100000; i++ {
		s := newSeries(uint64(i), m, "cpu", models.Tags{models.NewTag(
			[]byte("host"),
			[]byte(fmt.Sprintf("host%d", i)))})
		m.addSeries(s)
	}

	if exp, got := 100000, len(m.seriesKeys()); exp != got {
		b.Fatalf("series count mismatch: exp %v got %v", exp, got)
	}

	stmt, err := influxql.NewParser(strings.NewReader(`SELECT * FROM cpu WHERE host =~ /host\d+/`)).ParseStatement()
	if err != nil {
		b.Fatalf("invalid statement: %s", err)
	}

	selectStmt := stmt.(*influxql.SelectStatement)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, _, _ := m.idsForExpr(selectStmt.Condition.(*influxql.BinaryExpr))
		if exp, got := 100000, len(ids); exp != got {
			b.Fatalf("series count mismatch: exp %v got %v", exp, got)
		}

	}
}

func BenchmarkMeasurement_SeriesIDForExp_NERegex(b *testing.B) {
	m := newMeasurement("foo", "cpu")
	for i := 0; i < 100000; i++ {
		s := newSeries(uint64(i), m, "cpu", models.Tags{models.Tag{
			Key:   []byte("host"),
			Value: []byte(fmt.Sprintf("host%d", i))}})
		m.addSeries(s)
	}

	if exp, got := 100000, len(m.seriesKeys()); exp != got {
		b.Fatalf("series count mismatch: exp %v got %v", exp, got)
	}

	stmt, err := influxql.NewParser(strings.NewReader(`SELECT * FROM cpu WHERE host !~ /foo\d+/`)).ParseStatement()
	if err != nil {
		b.Fatalf("invalid statement: %s", err)
	}

	selectStmt := stmt.(*influxql.SelectStatement)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ids, _, _ := m.idsForExpr(selectStmt.Condition.(*influxql.BinaryExpr))
		if exp, got := 100000, len(ids); exp != got {
			b.Fatalf("series count mismatch: exp %v got %v", exp, got)
		}

	}

}

func benchmarkTagSets(b *testing.B, n int, opt query.IteratorOptions) {
	m := newMeasurement("foo", "m")
	ss := tsdb.NewSeriesIDSet()

	for i := 0; i < n; i++ {
		tags := map[string]string{"tag1": "value1", "tag2": "value2"}
		s := newSeries(uint64(i), m, fmt.Sprintf("m,tag1=value1,tag2=value2"), models.NewTags(tags))
		ss.Add(uint64(i))
		m.addSeries(s)
	}

	// warm caches
	m.tagSets(ss, opt)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.tagSets(ss, opt)
	}
}

func BenchmarkMeasurement_TagSetsNoDimensions_1000(b *testing.B) {
	benchmarkTagSets(b, 1000, query.IteratorOptions{})
}

func BenchmarkMeasurement_TagSetsDimensions_1000(b *testing.B) {
	benchmarkTagSets(b, 1000, query.IteratorOptions{Dimensions: []string{"tag1", "tag2"}})
}

func BenchmarkMeasurement_TagSetsNoDimensions_100000(b *testing.B) {
	benchmarkTagSets(b, 100000, query.IteratorOptions{})
}

func BenchmarkMeasurement_TagSetsDimensions_100000(b *testing.B) {
	benchmarkTagSets(b, 100000, query.IteratorOptions{Dimensions: []string{"tag1", "tag2"}})
}
