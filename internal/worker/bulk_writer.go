package worker

import (
	"context"
	"database/sql"
	"log"
	"sync/atomic"

	"txt-to-sqlserver/internal/metrics"
	"txt-to-sqlserver/internal/model"

	mssql "github.com/microsoft/go-mssqldb"
)

func bulkInsert(
	ctx context.Context,
	db *sql.DB,
	table string,
	cols []string,
	data <-chan func() []any,
	done chan<- struct{},
) {
	defer close(done)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(mssql.CopyIn(table, mssql.BulkOptions{}, cols...))
	if err != nil {
		log.Fatal(err)
	}

	for row := range data {
		_, err := stmt.Exec(row()...)
		if err != nil {
			log.Fatal(err)
		}
		atomic.AddInt64(&metrics.InsertedRows, 1)
	}

	_, _ = stmt.Exec()
	_ = stmt.Close()
	_ = tx.Commit()
}

/* =========================
   PUBLIC BULK WRITERS
========================= */

func Bulk130(ctx context.Context, db *sql.DB, ch <-chan model.Sdeal130Hdr, done chan<- struct{}) {
	rows := make(chan func() []any, 1000)
	go bulkInsert(ctx, db, "dbo.sdeal_130_hdr",
		[]string{
			"block_id", "block_code", "conditiontype",
			"keycombination", "keycomb",
			"salesorganization", "distributionchannel",
			"salesoffice", "division", "paymentterm",
			"customer", "material", "attribut2",
			"validuntil", "validfrom", "conditionrecordno",
			"filename", "linenumber", "cdate",
		},
		rows, done,
	)

	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.BlockID, r.BlockCode, r.ConditionType,
				r.KeyCombination, r.KeyComb,
				r.SalesOrganization, r.DistributionChannel,
				r.SalesOffice, r.Division, r.PaymentTerm,
				r.Customer, r.Material, r.Attribut2,
				r.ValidUntil, r.ValidFrom, r.ConditionRecordNo,
				r.FileName, r.LineNumber, r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk131(ctx context.Context, db *sql.DB, ch <-chan model.Sdeal131Det, done chan<- struct{}) {
	rows := make(chan func() []any, 1000)
	go bulkInsert(ctx, db, "dbo.sdeal_131_det",
		[]string{
			"block_id", "block_code", "conditionrecordno",
			"scale", "unit", "amount", "currency",
			"filename", "linenumber", "cdate",
		},
		rows, done,
	)
	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.BlockID, r.BlockCode, r.ConditionRecordNo,
				r.Scale, r.Unit, r.Amount, r.Currency,
				r.FileName, r.LineNumber, r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk132(ctx context.Context, db *sql.DB, ch <-chan model.Sdeal132Mix, done chan<- struct{}) {
	rows := make(chan func() []any, 1000)
	go bulkInsert(ctx, db, "dbo.sdeal_132_mix",
		[]string{
			"block_id", "block_code", "mix_code",
			"seq_no", "level_no", "plant", "material",
			"scale", "filename", "linenumber", "cdate",
		},
		rows, done,
	)
	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.BlockID, r.BlockCode, r.MixCode,
				r.SeqNo, r.LevelNo, r.Plant, r.Material,
				r.Scale, r.FileName, r.LineNumber, r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk120(ctx context.Context, db *sql.DB, ch <-chan model.Sdeal120Hdr, done chan<- struct{}) {
	rows := make(chan func() []any, 1000)
	go bulkInsert(ctx, db, "dbo.sdeal_120_hdr",
		[]string{
			"block_id", "block_code", "conditiontype",
			"salesorganization", "distributionchannel",
			"customer", "validuntil", "validfrom",
			"conditionrecordno", "filename", "linenumber", "cdate",
		},
		rows, done,
	)
	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.BlockID, r.BlockCode, r.ConditionType,
				r.SalesOrganization, r.DistributionChannel,
				r.Customer, r.ValidUntil, r.ValidFrom,
				r.ConditionRecordNo, r.FileName, r.LineNumber, r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk121(ctx context.Context, db *sql.DB, ch <-chan model.Sdeal121Itm, done chan<- struct{}) {
	rows := make(chan func() []any, 1000)
	go bulkInsert(ctx, db, "dbo.sdeal_121_itm",
		[]string{
			"block_id", "block_code", "material",
			"salesorganization", "distributionchannel",
			"customer", "validuntil", "validfrom",
			"conditionrecordno", "flag",
			"filename", "linenumber", "cdate",
		},
		rows, done,
	)
	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.BlockID, r.BlockCode, r.Material,
				r.SalesOrganization, r.DistributionChannel,
				r.Customer, r.ValidUntil, r.ValidFrom,
				r.ConditionRecordNo, r.Flag,
				r.FileName, r.LineNumber, r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk122(ctx context.Context, db *sql.DB, ch <-chan model.Sdeal122Det, done chan<- struct{}) {
	rows := make(chan func() []any, 1000)
	go bulkInsert(ctx, db, "dbo.sdeal_122_det",
		[]string{
			"block_id", "block_code", "value",
			"percent_flag", "plant", "flag",
			"filename", "linenumber", "cdate",
		},
		rows, done,
	)
	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.BlockID, r.BlockCode, r.Value,
				r.PercentFlg, r.Plant, r.Flag,
				r.FileName, r.LineNumber, r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk123(ctx context.Context, db *sql.DB, ch <-chan model.Sdeal123Mix, done chan<- struct{}) {
	rows := make(chan func() []any, 1000)
	go bulkInsert(ctx, db, "dbo.sdeal_123_mix",
		[]string{
			"block_id", "block_code", "conditiontype",
			"salesorganization", "distributionchannel",
			"customer", "material", "validuntil",
			"validfrom", "conditionrecordno", "qty",
			"plant", "flag",
			"filename", "linenumber", "cdate",
		},
		rows, done,
	)
	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.BlockID, r.BlockCode, r.ConditionType,
				r.SalesOrganization, r.DistributionChannel,
				r.Customer, r.Material, r.ValidUntil,
				r.ValidFrom, r.ConditionRecordNo, r.Qty,
				r.Plant, r.Flag,
				r.FileName, r.LineNumber, r.CDate,
			}
		}
	}
	close(rows)
}

func Bulk124(ctx context.Context, db *sql.DB, ch <-chan model.Sdeal124Reg, done chan<- struct{}) {
	rows := make(chan func() []any, 1000)
	go bulkInsert(ctx, db, "dbo.sdeal_124_reg",
		[]string{
			"block_id", "block_code", "conditionrecordno",
			"scale_no", "unit", "rate", "currency",
			"filename", "linenumber", "cdate",
		},
		rows, done,
	)
	for r := range ch {
		r := r
		rows <- func() []any {
			return []any{
				r.BlockID, r.BlockCode, r.ConditionRecordNo,
				r.ScaleNo, r.Unit, r.Rate, r.Currency,
				r.FileName, r.LineNumber, r.CDate,
			}
		}
	}
	close(rows)
}
