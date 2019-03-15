package main

import (
	"fmt"
	"time"
)

type CategoriesDto struct {
	Values    [][]int64 `json:"values"`
	TotalSize int64     `json:"totalsizes"`
}

type Categories struct {
	CategoriesDto
}

type BoundsElement struct {
	Name        string `json:"name"`        // unique ID
	Description string `json:"description"` // valid Department.name
	Value       int64  `json:"value"`       // age or size
	Index       int    `json:"index"`       // const iota value
}

type BoundsType int64
type FileSizeBounds BoundsType

const (
	SizeAny FileSizeBounds = iota
	SizeGt1MB
	SizeGt10MB
	SizeGt100MB
	SizeGt1GB
	SizeGt10GB
	SizeGt100GB
)

type FileAgeBounds BoundsType

const (
	AnyAge FileAgeBounds = iota
	ThirtyDays
	NinetyDays
	SixMonths
	OneYear
	TwoYears
	SevenYears
)

var BoundsSizes = []BoundsElement{
	{Name: "SizeAny", Description: "<100K", Index: int(SizeAny), Value: 0},
	{Name: "SizeGt1MB", Description: ">1MB", Index: int(SizeGt1MB), Value: 1024 * 1024},
	{Name: "SizeGt10MB", Description: ">10MB", Index: int(SizeGt10MB), Value: 10 * 1024 * 1024},
	{Name: "SizeGt100MB", Description: ">100MB", Index: int(SizeGt100MB), Value: 100 * 1024 * 1024},
	{Name: "SizeGt1GB", Description: ">1GB", Index: int(SizeGt1GB), Value: 1024 * 1024 * 1024},
	{Name: "SizeGt10GB", Description: ">10GB", Index: int(SizeGt10GB), Value: 10 * 1024 * 1024 * 1024},
	{Name: "SizeGt100GB", Description: ">100GB", Index: int(SizeGt100GB), Value: 100 * 1024 * 1024 * 1024},
}

var BoundsAges = []BoundsElement{
	{Name: "LessThirtyDays", Description: "<30 days", Index: int(AnyAge), Value: 0},
	{Name: "ThirtyDays", Description: "30 days", Index: int(ThirtyDays), Value: 30},
	{Name: "NinetyDays", Description: "90 days", Index: int(NinetyDays), Value: 90},
	{Name: "SixMonths", Description: "6 mo", Index: int(SixMonths), Value: 180},
	{Name: "OneYear", Description: "1 yr", Index: int(OneYear), Value: 365},
	{Name: "TwoYears", Description: "2 yr", Index: int(TwoYears), Value: 730},
	{Name: "SevenYears", Description: "7 yr", Index: int(SevenYears), Value: 2555},
}

func NewCategories() *Categories {
	var cat Categories

	// allocate category storage
	cat.Values = newCategoryValues()
	return &cat
}

func newCategoryValues() [][]int64 {
	values := make([][]int64, len(BoundsAges))
	for ages := 0; ages < len(BoundsAges); ages++ {
		values[ages] = make([]int64, len(BoundsSizes))
	}

	return values
}

func (categories *Categories) ToDto() *CategoriesDto {
	return &categories.CategoriesDto
}

// adds the lower entry into this category
func (categories *Categories) AddTo(lower *Categories) {
	// loop through the ages
	for ages := 0; ages < len(BoundsAges); ages++ {
		for sizes := 0; sizes < len(BoundsSizes); sizes++ {
			categories.Values[ages][sizes] += lower.Values[ages][sizes]
		}
	}
	categories.TotalSize += lower.TotalSize
}

// categorizes a file and adds its size into the entry
func (categories *Categories) CategorizeFile(fileTime *time.Time, fileSize int64) {
	age := ageBucketFromFileTime(fileTime)
	size := sizeBucketFromFileSize(fileSize)

	// fill in right spot
	categories.Values[age][size] += fileSize

	// file size is always added to the 0,0 entry
	categories.TotalSize += fileSize
}

// Calculate age index from top down of BoundAges slice
func ageBucketFromFileTime(fileTime *time.Time) FileAgeBounds {
	var age FileAgeBounds
	for age = SevenYears; age > AnyAge; age-- {
		// use selected time and compare if greater
		if time.Since(*fileTime).Hours()/24.0 >= float64(BoundsAges[age].Value) {
			break
		}
	}

	return age
}

// Calculate size index from top down of FileSizeBounds slice
func sizeBucketFromFileSize(fileSize int64) FileSizeBounds {
	var size FileSizeBounds
	for size = SizeGt100GB; size > SizeAny; size-- {
		// use selected time and compare if greater
		if fileSize >= BoundsSizes[size].Value {
			break
		}
	}

	return size
}

func (categories *Categories) CategorizeByAgeAndSize(fileTime *time.Time, fileSize int64) Categories {
	// Make a copy of the categories, in the interest of concurrency safety
	categoryValues := newCategoryValues()
	for ages := 0; ages < len(BoundsAges); ages++ {
		for sizes := 0; sizes < len(BoundsSizes); sizes++ {
			categoryValues[ages][sizes] = categories.Values[ages][sizes]
		}
	}

	age := ageBucketFromFileTime(fileTime)
	size := sizeBucketFromFileSize(fileSize)

	// fill in right spot
	categoryValues[age][size] += fileSize

	categoryTotalSize := categories.TotalSize + fileSize

	return Categories{CategoriesDto: CategoriesDto{Values: categoryValues, TotalSize: categoryTotalSize}}
}

func (categories *Categories) String() string {
	result := ""
	for _, i := range categories.Values {
		for _, j := range i {
			result += fmt.Sprintf("%10d", j)
		}
		result += "\n"
	}
	return result
}
