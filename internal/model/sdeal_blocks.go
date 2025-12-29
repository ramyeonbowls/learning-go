package model

import "time"

// ===== Block 130 =====
type Sdeal130Hdr struct {
	BlockID             string
	BlockCode           string
	ConditionType       string
	KeyCombination      string
	KeyComb             string
	SalesOrganization   string
	DistributionChannel string
	SalesOffice         string
	Division            string
	PaymentTerm         string
	Customer            string
	Material            string
	Attribut2           string
	ValidUntil          string
	ValidFrom           string
	ConditionRecordNo   string
	FileName            string
	LineNumber          int
	CDate               time.Time
}

// ===== Block 131 =====
type Sdeal131Det struct {
	BlockID           string
	BlockCode         string
	ConditionRecordNo string
	Scale             string
	Unit              string
	Amount            string
	Currency          string
	FileName          string
	LineNumber        int
	CDate             time.Time
}

// ===== Block 132 =====
type Sdeal132Mix struct {
	BlockID    string
	BlockCode  string
	MixCode    string
	SeqNo      string
	LevelNo    string
	Plant      string
	Material   string
	Scale      string
	FileName   string
	LineNumber int
	CDate      time.Time
}

// ===== Block 120 =====
type Sdeal120Hdr struct {
	BlockID             string
	BlockCode           string
	ConditionType       string
	SalesOrganization   string
	DistributionChannel string
	Customer            string
	ValidUntil          string
	ValidFrom           string
	ConditionRecordNo   string
	FileName            string
	LineNumber          int
	CDate               time.Time
}

// ===== Block 121 =====
type Sdeal121Itm struct {
	BlockID             string
	BlockCode           string
	Material            string
	SalesOrganization   string
	DistributionChannel string
	Customer            string
	ValidUntil          string
	ValidFrom           string
	ConditionRecordNo   string
	Flag                string
	FileName            string
	LineNumber          int
	CDate               time.Time
}

// ===== Block 122 =====
type Sdeal122Det struct {
	BlockID    string
	BlockCode  string
	Value      string
	PercentFlg string
	Plant      string
	Flag       string
	FileName   string
	LineNumber int
	CDate      time.Time
}

// ===== Block 123 =====
type Sdeal123Mix struct {
	BlockID             string
	BlockCode           string
	ConditionType       string
	SalesOrganization   string
	DistributionChannel string
	Customer            string
	Material            string
	ValidUntil          string
	ValidFrom           string
	ConditionRecordNo   string
	Qty                 string
	Plant               string
	Flag                string
	FileName            string
	LineNumber          int
	CDate               time.Time
}

// ===== Block 124 =====
type Sdeal124Reg struct {
	BlockID           string
	BlockCode         string
	ConditionRecordNo string
	ScaleNo           string
	Unit              string
	Rate              string
	Currency          string
	FileName          string
	LineNumber        int
	CDate             time.Time
}
