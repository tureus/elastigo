package elastigo

import "encoding/json"

func Aggregate(name string) *AggregateDsl {
	return &AggregateDsl{Name: name}
}

type AggregateDsl struct {
	Name          string
	TypeName      string
	Type          interface{}
	Filters       *FilterWrap              `json:"filters,omitempty"`
	AggregatesVal map[string]*AggregateDsl `json:"aggregations,omitempty"`
}

type FieldAggregate struct {
	Field string `json:"field"`
	Size  *int   `json:"size,omitempty"`
	Order *FieldOrder `json:"order,omitempty"`
}

type FieldOrder struct {
	Term string `json:"_term"`
}

/**
 * Aggregates accepts n "sub-aggregates" to be applied to this aggregate
 *
 * agg := Aggregate("user").Term("user_id")
 * agg.Aggregates(
 *   Aggregate("total_spent").Sum("price"),
 *   Aggregate("total_saved").Sum("discount"),
 * )
 */
func (d *AggregateDsl) Aggregates(aggs ...*AggregateDsl) *AggregateDsl {
	if len(aggs) < 1 {
		return d
	}
	if len(d.AggregatesVal) == 0 {
		d.AggregatesVal = make(map[string]*AggregateDsl)
	}

	for _, agg := range aggs {
		d.AggregatesVal[agg.Name] = agg
	}
	return d
}

func (d *AggregateDsl) Min(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "min"
	return d
}

func (d *AggregateDsl) Max(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "max"
	return d
}

func (d *AggregateDsl) Sum(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "sum"
	return d
}

type DerivativeAggregate struct {
	BucketsPath string `json:"buckets_path"`
}

func (d *AggregateDsl) CumulativeSum(path string) *AggregateDsl {
	d.Type = DerivativeAggregate{BucketsPath: path}
	d.TypeName = "cumulative_sum"
	return d
}

func (d *AggregateDsl) Avg(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "avg"
	return d
}

func (d *AggregateDsl) Stats(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "stats"
	return d
}

func (d *AggregateDsl) ExtendedStats(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "extended_stats"
	return d
}

func (d *AggregateDsl) ValueCount(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "value_count"
	return d
}

func (d *AggregateDsl) Percentiles(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "percentiles"
	return d
}

type Cardinality struct {
	Field              string  `json:"field"`
	PrecisionThreshold float64 `json:"precision_threshold,omitempty"`
	Rehash             bool    `json:"rehash,omitempty"`
}

/**
 * Cardinality(
 *	 "field_name",
 *	 true,
 *   0,
 * )
 */
func (d *AggregateDsl) Cardinality(field string, rehash bool, threshold int) *AggregateDsl {
	c := Cardinality{Field: field}

	// Only set if it's false, since the default is true
	if !rehash {
		c.Rehash = false
	}

	if threshold > 0 {
		c.PrecisionThreshold = float64(threshold)
	}
	d.Type = c
	d.TypeName = "cardinality"
	return d
}

type GeohashGrid struct {
	Field string `json:"field"`
	Precision int `json:"precision"`
}

func (d *AggregateDsl) GeohashGrid(field string, precision int) *AggregateDsl {
	d.TypeName = "geohash_grid"
	d.Type = GeohashGrid {
		Field: field,
		Precision: precision,
	}

	return d
}

func (d *AggregateDsl) Global() *AggregateDsl {
	d.Type = struct{}{}
	d.TypeName = "global"
	return d
}

func (d *AggregateDsl) Filter(filters ...interface{}) *AggregateDsl {

	if len(filters) == 0 {
		return d
	}

	if d.Filters == nil {
		d.Filters = NewFilterWrap()
	}

	d.Filters.addFilters(filters)
	return d
}

func (d *AggregateDsl) Missing(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "missing"
	return d
}

func (d *AggregateDsl) Terms(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "terms"
	return d
}

func (d *AggregateDsl) TermsWithSize(field string, size int) *AggregateDsl {
	d.Type = FieldAggregate{Field: field, Size: &size}
	d.TypeName = "terms"
	return d
}

func (d *AggregateDsl) TermsWithSizeAndOrder(field string, size int, order string) *AggregateDsl {
	d.Type = FieldAggregate{ Field: field, Size: &size, Order: &FieldOrder{ Term: order } }
	d.TypeName = "terms"
	return d
}

func (d *AggregateDsl) SignificantTerms(field string) *AggregateDsl {
	d.Type = FieldAggregate{Field: field}
	d.TypeName = "significant_terms"
	return d
}

type Histogram struct {
	Field    string  `json:"field"`
	Interval float64 `json:"interval"`
}

func (d *AggregateDsl) Histogram(field string, interval int) *AggregateDsl {
	d.Type = Histogram{
		Field:    field,
		Interval: float64(interval),
	}
	d.TypeName = "histogram"
	return d
}

type DateHistogram struct {
	Field    string `json:"field"`
	Interval string `json:"interval"`
}

func (d *AggregateDsl) DateHistogram(field, interval string) *AggregateDsl {
	d.Type = DateHistogram{
		Field:    field,
		Interval: interval,
	}
	d.TypeName = "date_histogram"
	return d
}

// Force buckets, empty or not, to be generated for all units in interval
type MaximalDateHistogram struct {
	Field    string `json:"field"`
	Interval string `json:"interval"`
	Format   string `json:"format,omitempty"`
	MinDocCount int `json:"min_doc_count"`
	TimeZone string `json:"time_zone,omitempty"`
	ExtendedBounds MaximalDateHistogramExtendedBounds `json:"extended_bounds"`
}

type MaximalDateHistogramExtendedBounds struct{
	Min interface{} `json:"min"`
	Max interface{} `json:"max"`
}

func (d *AggregateDsl) MaximalDateHistogram(field, interval, format, timezone string, bounds_min, bounds_max interface{}) *AggregateDsl {
	d.Type = MaximalDateHistogram{
		Field: field,
		Interval: interval,
		Format: format,
		TimeZone: timezone,
		MinDocCount: 0,
		ExtendedBounds: MaximalDateHistogramExtendedBounds{
			Min: bounds_min,
			Max: bounds_max,
		},
	}
	d.TypeName = "date_histogram"
	return d
}

// Force buckets, empty or not, to be generated for all units in interval from indexed script
type MaximalDateHistogramFromIndexedScriptParams struct {
	Interval int `json:"interval"`
}
type MaximalDateHistogramFromIndexedScript struct {
	ScriptId    string `json:"script_id"`
	Interval string `json:"interval"`
	Params MaximalDateHistogramFromIndexedScriptParams `json:"params"`
	Format   string `json:"format,omitempty"`
	MinDocCount int `json:"min_doc_count"`
	ExtendedBounds MaximalDateHistogramExtendedBounds `json:"extended_bounds"`
}

func (d *AggregateDsl) MaximalDateHistogramFromIndexedScript(script_id, interval string, interval_s int, format string, bounds_min, bounds_max interface{}) *AggregateDsl {
	d.Type = MaximalDateHistogramFromIndexedScript{
		ScriptId: script_id,
		Interval: interval,
		Params: MaximalDateHistogramFromIndexedScriptParams {
			Interval: interval_s,
		},
		Format: format,
		MinDocCount: 0,
		ExtendedBounds: MaximalDateHistogramExtendedBounds{
			Min: bounds_min,
			Max: bounds_max,
		},
	}
	d.TypeName = "date_histogram"
	return d
}


type NestedPath struct {
	Path string `json:"path"`
}

func (d *AggregateDsl) NestedPath(path string) *AggregateDsl {
	d.Type = NestedPath{
		Path: path,
	}
	d.TypeName = "nested"
	return d
}

func (d *AggregateDsl) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.toMap())
}

func (d *AggregateDsl) toMap() map[string]interface{} {
	root := map[string]interface{}{}

	if d.Type != nil {
		root[d.TypeName] = d.Type
	}
	aggregates := d.aggregatesMap()

	if d.Filters != nil {
		root["filter"] = d.Filters
	}

	if len(aggregates) > 0 {
		root["aggregations"] = aggregates
	}
	return root

}

func (d *AggregateDsl) aggregatesMap() map[string]interface{} {
	root := map[string]interface{}{}

	if len(d.AggregatesVal) > 0 {
		for _, agg := range d.AggregatesVal {
			root[agg.Name] = agg.toMap()
		}
	}
	return root
}
