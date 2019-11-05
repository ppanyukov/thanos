package storepb

import (
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
)

func SeriesPbToPtr(series *Series) *SeriesPtr {
	return &SeriesPtr{
		Labels: LabelPbSliceToPtr(series.Labels),
		Chunks: series.Chunks,
	}
}

func LabelPtrSliceToPb(labels []LabelPtr) []Label {
	res := make([]Label, 0, len(labels))
	for _, label := range labels {
		res = append(res, *label.ToPb())
	}
	return res
}

func LabelPbSliceToPtr(labels []Label) []LabelPtr {
	res := make([]LabelPtr, 0, len(labels))
	for _, label := range labels {
		labelPtr := LabelPtr{
			Name:  label.Name,
			Value: label.Value,
		}
		res = append(res, labelPtr)
	}
	return res
}

func LabelSetPtrSliceToPb(labelSets []LabelSetPtr) []LabelSet {
	res := make([]LabelSet, 0, len(labelSets))
	for _, labelSet := range labelSets {
		res = append(res, *labelSet.ToPb())
	}
	return res
}


func LabelSetPbSliceToPtr(labelSets []LabelSet) []LabelSetPtr {
	res := make([]LabelSetPtr, 0, len(labelSets))
	for _, labelSet := range labelSets {
		labelSetPtr := LabelSetPtr{
			Labels:LabelPbSliceToPtr(labelSet.Labels),
		}
		res = append(res, labelSetPtr)
	}
	return res
}


type LabelPtr struct {
	Name  string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (label *LabelPtr) ToPb() *Label {
	return &Label{
		Name:  label.Name,
		Value: label.Value,
	}
}

func (label *LabelPtr) String() string {
	return label.ToPb().String()
}

type LabelSetPtr struct {
	Labels []LabelPtr `protobuf:"bytes,1,rep,name=labels,proto3" json:"labels"`
}

func (lbs *LabelSetPtr) ToPb() *LabelSet {
	return &LabelSet{
		Labels: LabelPtrSliceToPb(lbs.Labels),
	}
}

func (lbs *LabelSetPtr) String() string {
	return lbs.ToPb().String()
}

type SeriesPtr struct {
	Labels []LabelPtr  `protobuf:"bytes,1,rep,name=labels,proto3" json:"labels"`
	Chunks []AggrChunk `protobuf:"bytes,2,rep,name=chunks,proto3" json:"chunks"`
}

func (s *SeriesPtr) ToPb() *Series {
	return &Series{
		Labels: LabelPtrSliceToPb(s.Labels),
		Chunks: s.Chunks,
	}
}

var PartialResponseStrategyValues = func() []string {
	var s []string
	for k := range PartialResponseStrategy_value {
		s = append(s, k)
	}
	return s
}()

func NewWarnSeriesResponse(err error) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Warning{
			Warning: err.Error(),
		},
	}
}

func NewSeriesResponse(series *SeriesPtr) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Series{
			Series: series.ToPb(),
		},
	}
}

// CompareLabels compares two sets of labels.
func CompareLabels(a, b []LabelPtr) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}
	for i := 0; i < l; i++ {
		if d := strings.Compare(a[i].Name, b[i].Name); d != 0 {
			return d
		}
		if d := strings.Compare(a[i].Value, b[i].Value); d != 0 {
			return d
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}

type emptySeriesSet struct{}

func (emptySeriesSet) Next() bool                    { return false }
func (emptySeriesSet) At() ([]LabelPtr, []AggrChunk) { return nil, nil }
func (emptySeriesSet) Err() error                    { return nil }

// EmptySeriesSet returns a new series set that contains no series.
func EmptySeriesSet() SeriesSet {
	return emptySeriesSet{}
}

// MergeSeriesSets returns a new series set that is the union of the input sets.
func MergeSeriesSets(all ...SeriesSet) SeriesSet {
	switch len(all) {
	case 0:
		return emptySeriesSet{}
	case 1:
		return all[0]
	}
	h := len(all) / 2

	return newMergedSeriesSet(
		MergeSeriesSets(all[:h]...),
		MergeSeriesSets(all[h:]...),
	)
}

// SeriesSet is a set of series and their corresponding chunks.
// The set is sorted by the label sets. Chunks may be overlapping or expected of order.
type SeriesSet interface {
	Next() bool
	At() ([]LabelPtr, []AggrChunk)
	Err() error
}

// mergedSeriesSet takes two series sets as a single series set.
type mergedSeriesSet struct {
	a, b SeriesSet

	lset         []LabelPtr
	chunks       []AggrChunk
	adone, bdone bool
}

// newMergedSeriesSet takes two series sets as a single series set.
// SeriesPtr that occur in both sets should have disjoint time ranges.
// If the ranges overlap b samples are appended to a samples.
// If the single SeriesSet returns same series within many iterations,
// merge series set will not try to merge those.
func newMergedSeriesSet(a, b SeriesSet) *mergedSeriesSet {
	s := &mergedSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedSeriesSet) At() ([]LabelPtr, []AggrChunk) {
	return s.lset, s.chunks
}

func (s *mergedSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	lsetA, _ := s.a.At()
	lsetB, _ := s.b.At()
	return CompareLabels(lsetA, lsetB)
}

func (s *mergedSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()

	// Both sets contain the current series. Chain them into a single one.
	if d > 0 {
		s.lset, s.chunks = s.b.At()
		s.bdone = !s.b.Next()
	} else if d < 0 {
		s.lset, s.chunks = s.a.At()
		s.adone = !s.a.Next()
	} else {
		// Concatenate chunks from both series sets. They may be expected of order
		// w.r.t to their time range. This must be accounted for later.
		lset, chksA := s.a.At()
		_, chksB := s.b.At()

		s.lset = lset
		// Slice reuse is not generally safe with nested merge iterators.
		// We err on the safe side an create a new slice.
		s.chunks = make([]AggrChunk, 0, len(chksA)+len(chksB))
		s.chunks = append(s.chunks, chksA...)
		s.chunks = append(s.chunks, chksB...)

		s.adone = !s.a.Next()
		s.bdone = !s.b.Next()
	}
	return true
}

func LabelsToPromLabels(lset []LabelPtr) labels.Labels {
	ret := make(labels.Labels, len(lset))
	for i, l := range lset {
		ret[i] = labels.Label{Name: l.Name, Value: l.Value}
	}

	return ret
}

func LabelsToString(lset []LabelPtr) string {
	var s []string
	for _, l := range lset {
		s = append(s, l.String())
	}
	return "[" + strings.Join(s, ",") + "]"
}

func LabelSetsToString(lsets []LabelSetPtr) string {
	s := []string{}
	for _, ls := range lsets {
		s = append(s, LabelsToString(ls.Labels))
	}
	return strings.Join(s, "")
}
